"""The owner-layout proof's pure parts: scope checks, teardown and spec shape."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / ".github/scripts"))
import kernel_nvme_tcp_ci as kernel


def write_launch(path, root, port):
    """One generated owner launch: its host root and its public pool slot."""
    Path(path).write_text(json.dumps(
        {"host": {"root": str(root), "live_target_pool": {"public_port_base": port}}}))


def write_links(port, names):
    port.mkdir(parents=True)
    (port / "subsystems").mkdir()
    for index, name in enumerate(names):
        (port / "subsystems" / name).symlink_to(f"../../../subsystems/sub-{index}")


class LayoutDefect(unittest.TestCase):
    def configfs(self, root):
        (root / "ports").mkdir(parents=True)
        (root / "subsystems").mkdir()
        (root / "hosts").mkdir()
        return root

    def test_a_distinct_pool_per_database_is_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            write_links(root / "ports" / "1000",
                        [kernel.LAYOUT_SCOPE + "aa" * 16 + "-p0-sub-1"])
            write_links(root / "ports" / "1002",
                        [kernel.LAYOUT_SCOPE + "bb" * 16 + "-p0-sub-2"])
            for node, hexes, port in (("a", "aa" * 16, 1000), ("b", "bb" * 16, 1002)):
                write_launch(root / f"{node}.yaml", root / "roots" / hexes, port)
                (root / "roots" / hexes / f"db-{hexes}").mkdir(parents=True)
            nodes = [{"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")},
                     {"namespace": "b", "partition": 0, "launch": str(root / "b.yaml")}]
            with patch.object(kernel, "CONFIGFS", root):
                self.assertIsNone(kernel.layout_defect(
                    nodes, {"a/0": "aa" * 16, "b/0": "bb" * 16}))

    def test_a_foreign_subsystem_on_a_port_is_a_defect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            write_links(root / "ports" / "1000", [
                kernel.LAYOUT_SCOPE + "aa" * 16 + "-p0-sub-1",
                kernel.LAYOUT_SCOPE + "cc" * 16 + "-p0-sub-3"])
            node = {"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")}
            write_launch(node["launch"], root / "roots", 1000)
            (root / "roots" / f"db-{'aa' * 16}").mkdir(parents=True)
            with patch.object(kernel, "CONFIGFS", root):
                defect = kernel.layout_defect([node], {"a/0": "aa" * 16})
        self.assertIn("holds another database's subsystem", defect)

    def test_a_port_that_published_nothing_is_a_defect(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            node = {"namespace": "a", "partition": 0, "launch": str(root / "a.yaml")}
            write_launch(node["launch"], root / "roots", 1000)
            (root / "roots" / f"db-{'aa' * 16}").mkdir(parents=True)
            with patch.object(kernel, "CONFIGFS", root):
                defect = kernel.layout_defect([node], {"a/0": "aa" * 16})
        self.assertIn("published no subsystem", defect)

    def test_teardown_reports_the_residual_objects_of_each_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.configfs(Path(tmp))
            residual = kernel.LAYOUT_SCOPE + "aa" * 16 + "-p0-sub-1"
            (root / "subsystems" / residual).mkdir()
            write_links(root / "ports" / "1000", [residual])
            with patch.object(kernel, "CONFIGFS", root):
                found = kernel.layout_teardown_defect({"aa" * 16})
        self.assertEqual(len(found), 2)


class CampaignModules(unittest.TestCase):
    def test_the_deployment_and_spec_builders_load(self):
        deployment, specs = kernel.campaign_modules()
        self.assertEqual(deployment.MAX_DATABASES_PER_PARTITION, 8)
        self.assertEqual(specs.OWNERS, kernel.LAYOUT_OWNERS)

    def test_one_spec_names_one_database_and_its_two_receipts(self):
        deployment, specs = kernel.campaign_modules()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.object(deployment, "certificates",
                              lambda path, owners: path.mkdir()):
                config, _nodes = deployment.prepare(
                    root / "deployment", {name: {} for name in kernel.LAYOUT_NAMESPACES},
                    root / "remote",
                    shape_yamls={name: "key_spaces: []\n" for name in kernel.LAYOUT_NAMESPACES},
                    hosts=["127.0.0.1"] * kernel.LAYOUT_OWNERS,
                    owners=kernel.LAYOUT_OWNERS, port=kernel.LAYOUT_PORT,
                    transport="tcp", coordinator_cache=0, live_transport="tcp")
            name = kernel.LAYOUT_NAMESPACES[0]
            spec = kernel.layout_spec(specs, config["databases"][name], root, 0,
                                      reopen=True)
        options = spec["backend"]["options"]
        self.assertEqual(options["owners"], kernel.LAYOUT_OWNERS)
        self.assertEqual(options["external_remote_config"], config["databases"][name])
        self.assertEqual(options["external_owner_durability_receipts"],
                         [str(specs.receipt_path(root, "native", partition))
                          for partition in range(kernel.LAYOUT_OWNERS)])
        self.assertFalse(spec["phases"]["preload"])
        self.assertEqual(spec["workload"]["mix"]["read"], 100)


if __name__ == "__main__":
    unittest.main()
