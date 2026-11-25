"""Transport-independent Q1 request, receipt and ownership rules."""

from copy import deepcopy

from q1_manifest import MANDATORY, STATUSES, fields, integer, roles, validate_binding
from q1_package import digest, hex_string, relative_path, require

VERBS = {"validate", "start", "poll", "pause", "resume", "stop", "dump", "collect"}
PHASES = {
    "golden_mixed": ("prepare", "restart_prefix", "restart_catchup", "cut", "closed_reopen", "collect"),
    "golden_leased": ("prepare", "restart_prefix", "restart_catchup", "cut", "closed_reopen", "collect"),
    "index_native": ("build", "pilot", "overwrite", "cold_reopen", "verify", "collect"),
    "index_synchronous": ("build", "pilot", "overwrite", "cold_reopen", "verify", "collect"),
    "put_native": ("prepare", "measure", "collect"),
    "put_synchronous": ("prepare", "measure", "collect"),
    "replay_native": ("prepare", "measure", "collect"),
    "replay_synchronous": ("prepare", "measure", "collect"),
    "capacity_r3": ("control", "policy", "collect"),
    "anchor_land4": ("prepare", "measure", "collect"),
}


class Scope:
    def __init__(self, spec, seal_sha, placement, placement_sha):
        require(validate_binding(spec, seal_sha, placement) == placement_sha, "placement bytes changed")
        self.spec = deepcopy(spec)
        self.placement = deepcopy(placement)
        self.input_sha = {"seal": seal_sha, "placement": placement_sha}

    def identity(self, case, role):
        require(case in self.placement["case_configs"] and role in roles(case), "unadmitted role")
        slot, _ = roles(case)[role]
        config = self.placement["case_configs"][case]["roles"][role]
        return {"run_id": self.placement["run_id"], "case_id": case, "role": role,
                "instance_id": self.placement["slots"][slot]["id"], "validator_id": config["validator_id"],
                "input_sha": {**self.input_sha, "config": config["config_sha"]}}

    def request(self, case, phase, role, verb, deadline):
        identity = self.identity(case, role)
        require(verb in VERBS and phase in PHASES[case], "unadmitted operation")
        return {**identity, "phase": phase, "verb": verb, "deadline": deadline}

    def validate_request(self, request, instance_id, now):
        case, phase, role, verb, deadline = (request[k] for k in ("case_id", "phase", "role", "verb", "deadline"))
        require(request == self.request(case, phase, role, verb, deadline)
                and request["instance_id"] == instance_id, "request scope changed")
        start = self.placement["earliest_creation"]
        require(integer(deadline) and start <= now < deadline <= start + 12300, "stale or invalid deadline")
        unit_id = digest({k: request[k] for k in ("run_id", "case_id", "role", "instance_id", "input_sha")})
        return f"th-q1-{unit_id}.service"

    def validate_receipt(self, request, receipt):
        fields(receipt, "run_id case_id phase role instance_id validator_id input_sha started ended status artifacts",
               "receipt")
        for key in self.identity(request["case_id"], request["role"]).keys() | {"phase"}:
            require(receipt[key] == request[key], f"receipt {key} changed")
        require(integer(receipt["started"]) and integer(receipt["ended"])
                and self.placement["earliest_creation"] <= receipt["started"] <= receipt["ended"] <= request["deadline"]
                and receipt["status"] in STATUSES, "invalid receipt times or status")
        require(isinstance(receipt["artifacts"], list), "invalid receipt artifacts")
        require(receipt["status"] != "passed" or request["verb"] != "collect" or receipt["artifacts"],
                "missing contributor upload")
        prefix = (receipt["run_id"], receipt["case_id"], receipt["phase"], receipt["role"])
        seen = set()
        total = 0
        for item in receipt["artifacts"]:
            fields(item, "path sha256 generation bytes", "artifact")
            path = relative_path(item["path"])
            require(path.parts[:4] == prefix and len(path.parts) >= 5 and path not in seen
                    and hex_string(item["sha256"], 64) and integer(item["generation"]) and integer(item["bytes"], 0),
                    "unsafe, duplicate or unpinned artifact")
            seen.add(path)
            total += item["bytes"]
        require(total <= self.spec["artifact_bytes"], "receipt exceeds archive budget")
        return total


def stable_empty(inventory):
    """The caller supplies complete, scoped snapshots; None is a transient scan."""
    previous = None
    for _ in range(8):
        current = inventory()
        if current is not None and previous == current:
            require(not current, "NVMe residue from this case")
            return
        previous = current
    raise ValueError("Q1: scoped NVMe inventory did not stabilize")


def run_cases(scope, clock, execute, case_body, persist, inventory):
    """Drive ordered cases with one deadline and always clean possible starts.

    execute sends one admitted request and returns its immutable receipt. case_body
    implements workload-specific phase ordering using send. persist commits receipts
    and markers immutably. inventory scans only this case's admitted resources.
    """
    start = scope.placement["earliest_creation"]
    deadline = start + 12300
    cases = scope.spec["cases"]
    archive_bytes = 0
    persist("q1-started", {"run_id": scope.placement["run_id"], "input_sha": scope.input_sha})
    outcomes = dict.fromkeys((case["id"] for case in cases), "aborted")
    try:
        for offset, entry in enumerate(cases):
            case = entry["id"]
            require(clock() + sum(row["seconds"] for row in cases[offset:]) + 1620 <= deadline,
                    "remaining cases exceed fleet deadline")
            case_end = clock() + entry["seconds"]
            owned, collected, requests = {}, set(), set()

            def send(phase, role, verb, *, cleanup=False):
                nonlocal archive_bytes
                req = scope.request(case, phase, role, verb, deadline if cleanup else case_end)
                scope.validate_request(req, req["instance_id"], clock())
                if verb == "start":
                    owned[role] = phase
                receipt = execute(req)
                length = scope.validate_receipt(req, receipt)
                request_sha = digest(req)
                if request_sha not in requests:
                    archive_bytes += length
                    require(archive_bytes <= scope.spec["artifact_bytes"], "fleet exceeds archive budget")
                    persist(f"{case}/{request_sha}", receipt)
                    requests.add(request_sha)
                if receipt["status"] != "passed":
                    outcomes[case] = receipt["status"]
                    raise ValueError(f"Q1: {case}/{phase}/{role}: {receipt['status']}")
                if verb == "stop":
                    owned.pop(role, None)
                if verb == "collect":
                    collected.add(role)
                return receipt

            try:
                case_body(case, send)
                require(collected == set(roles(case)), "missing required contributors")
            finally:
                errors = []
                for role, phase in reversed(list(owned.items())):
                    try:
                        send(phase, role, "stop", cleanup=True)
                    except Exception as exc:
                        errors.append(str(exc))
                require(not errors, f"failed case cleanup: {errors}")
                stable_empty(lambda: inventory(case))
            outcomes[case] = "passed"
            active = {slot for slot, _ in roles(case).values()}
            idle = sorted(scope.placement["slots"].keys() - active)
            persist(f"{case}/terminal", {"status": "passed", "not_required": idle})
    finally:
        persist("q1-terminal", {"outcomes": outcomes, "input_sha": scope.input_sha})
