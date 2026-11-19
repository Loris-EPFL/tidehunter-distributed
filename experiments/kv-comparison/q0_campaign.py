"""One archive fleet: exact-commit put probes and instrumented land4 A/B."""
import re
from collections import deque

SELECTION = "nvmeof_q0"
BASELINE = "c4909d7ed18206ad210fc57afd7989883302896e"
F1 = "7afa3fb1a4347e14393e5c276de32e5fb65153e9"
COUNTER_PATCH_SHA256 = "1643a8947fd9b1cc07307685d5610873bae5a8c386569aa392d91b2aa018ac6d"
# First-parent production put-path candidates, with exact endpoint controls.
CANDIDATES = (
    F1,
    "6b572117a8c94ed814e6275b1fbb42e3e326c806",
    "d819c763d3cb52a04d6213b060719af746461b6c",
    "0d8170ee8e239922cdd4689b7f2015e28e862a19",
    "00f7a8d9779deb45e6fee9638371e70a80e8530a",
    "dec9852471a43873b040f786ff2a23771306d688",
    "db1cd37ff426761da76ef5d8875d0468e59685d6",
    "b57fd8e3dcaa8cffaa87216772a9b44ceb35d7c1",
    "2d6f79f26ab047115a737f4fc36afb3e3f24adc0",
    "103224563b489217f7123dc7f8bec1b43f5e79f3",
    "5eb998f1ec89b30711d627262432351f7c306c61",
    "78a782b792e3539b7de85be2891764cbaca641bb",
    "41d2fd951cff8887bbb7c398a012c3ba50d5bc96",
    "d2c19c4c66449d8695a80dbab0760d01559330a1",
    "b24218f732710347cc005e839852524e38792ebd",
    "4b3150274b101474535bffbc15447a53c03bcda0",
    BASELINE,
)
BISECT_REVISIONS = tuple(CANDIDATES[(i * (len(CANDIDATES) - 1) + 5) // 11] for i in range(12))
AB_ROLES = ("baseline_ab", "land4")
ROLES = (*BISECT_REVISIONS, *AB_ROLES)
RESERVATION_HOURS = 4
GUARD_MARGIN_SECONDS = 600
MINIMUM_GUARD_SLACK_SECONDS = 1500
PUT_MODEL_SECONDS = 80
REPLAY_MODEL_SECONDS = 350
REPEATS = 3
ARCHIVE_LIMIT_BYTES = 16_000_000_000


def validate_binaries(binaries):
    if not isinstance(binaries, dict) or set(binaries) != set(ROLES):
        raise ValueError("Q0 requires every selected binary role and no others")
    digests = {}
    for role, seal in binaries.items():
        instrumented = role in AB_ROLES
        fields = {"revision", "sha256"} | ({"instrumentation_patch_sha256"} if instrumented else set())
        if not isinstance(seal, dict) or set(seal) != fields:
            raise ValueError(f"Q0 invalid binary seal fields for {role}")
        for field, value in seal.items():
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{%d}" % (40 if field == "revision" else 64), value):
                raise ValueError(f"Q0 invalid {field} for {role}")
        expected = BASELINE if role == "baseline_ab" else role
        if role == "land4" and seal["revision"] in CANDIDATES or role != "land4" and seal["revision"] != expected:
            raise ValueError(f"Q0 incorrect source revision for {role}")
        previous = digests.get(seal["sha256"])
        if previous is not None and (instrumented or previous in AB_ROLES):
            raise ValueError(f"Q0 reused an executable across an instrumented A/B role: {previous}, {role}")
        digests[seal["sha256"]] = role
    if any(binaries[role]["instrumentation_patch_sha256"] != COUNTER_PATCH_SHA256 for role in AB_ROLES):
        raise ValueError("Q0 A/B requires the reviewed instrumentation patch")


def probe_order():
    """Breadth-first midpoint order; both controls run in the first block."""
    result = []
    pending = deque([(0, len(BISECT_REVISIONS) - 1)])
    while pending:
        lo, hi = pending.popleft()
        if hi - lo > 1:
            mid = (lo + hi) // 2
            result.append(BISECT_REVISIONS[mid])
            pending.extend(((lo, mid), (mid, hi)))
    return (result[0], F1, BASELINE, *result[1:])


def select_cases(jobs, binaries):
    """Run each exact historical ELF once per repeat, preferring endpoint labels."""
    validate_binaries(binaries)
    groups = {}
    for revision in (F1, BASELINE, *BISECT_REVISIONS[1:-1]):
        groups.setdefault(binaries[revision]["sha256"], []).append(revision)
    attribution = {revisions[0]: revisions for revisions in groups.values()}
    selected = [job for job in jobs if job["binary_role"] in AB_ROLES or job["binary_role"] in attribution]
    return selected, attribution


def jobs(p13_jobs, clone):
    sources = {(row['case_kind'], row['durability_tier']): row for row in p13_jobs
               if row['selection'] == 'nvmeof_p13_write_engine' and row.get('arm') == 'prewritten'
               and ((row['group'] == 'G35W' and row['concurrency'] == 64)
                    or (row['group'] == 'G35S' and row['concurrency'] == 16
                        and 'checkpoint_pipeline_depth' not in row))}
    matrix = []

    def add(kind, tier, role, repeat):
        source = sources[kind, tier]
        group = 'G37W' if kind == 'remote_put' else 'G37S'
        clone(matrix, SELECTION, source, group, binary_role=role, repeat=repeat,
              arm=role if role in AB_ROLES else 'bisect',
              profile=f'{group}-{tier}-{role if role in AB_ROLES else role[:8]}')

    bisect = probe_order()
    ab = [(kind, tier, role) for kind in ('remote_put', 'mainnet_replay')
          for tier in ('native', 'synchronous') for role in AB_ROLES]
    for repeat, order in enumerate((bisect, bisect[::-1], bisect[6:] + bisect[:6])):
        for role in order:
            add('remote_put', 'native', role, repeat)
        for cell in (ab if repeat != 1 else reversed(ab)):
            add(*cell, repeat)
    return matrix


def overhead_counts(evidence, elapsed_ns, tier):
    """Use the full successful measurement population, not whole case wall time."""
    if type(elapsed_ns) is not int or elapsed_ns <= 0:
        raise ValueError("Q0 measurement window must be positive")
    if evidence.get("measurement_drained") is not True or evidence.get("measurement_attempted") is not True:
        raise ValueError("Q0 requires complete before/after counter samples")
    start, end = (evidence[edge] for edge in ("start", "end"))
    owners = (start["owner_storage"], end["owner_storage"])
    cuts = (start["durable_cut"], end["durable_cut"])
    if (any(row.get("available") is not True for row in owners)
            or any(row.get("enabled") is not (tier == "native") for row in cuts)
            or cuts[0].get("epoch") != cuts[1].get("epoch")):
        raise ValueError("Q0 owner or durable-cut counter population unavailable or changed")

    def difference(rows, field):
        a, b = (row[field] for row in rows)
        if type(a) is not int or type(b) is not int or not 0 <= a <= b < 2**64 - 1:
            raise ValueError(f"Q0 {field} counter absent, decreased or saturated")
        return b - a

    return {"elapsed_ns": elapsed_ns, "counts": {
        "live_frontier_installs": difference(owners, "live_frontier_installs"),
        "durable_cut_nudges": difference(cuts, "nudges"),
    }}


def qualify_overhead(read_interval, window, tier):
    """An unavailable diagnostic invalidates this measurement, not the fleet."""
    try:
        return overhead_counts(read_interval["observed"], window["elapsed_ns"], tier)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        return {"available": False, "reason": f"{type(exc).__name__}: {exc}"}
