import unittest

import analyze
import cloud_campaign


class SealedTransportAccountingTests(unittest.TestCase):
    def test_kernel_tcp_and_rdma_use_same_sealed_evidence(self):
        counters = {"eligible_hits": 100, "ineligible_fallbacks": 0,
                    "io_submissions": 100, "io_completions": 100,
                    "io_errors": 0, "io_timeouts": 0}
        media = {"schema_version": 1, "case_id": "case", "expected": "cold",
                 "status": "qualified", "reason_codes": [], "measurement_drained": True,
                 "owner_devices": [{"owner_index": 0, "major": 8, "minor": 0,
                                    "read_ops_before": 10, "read_ops_after": 110}],
                 "successful_reads": 100, "read_ops_per_success": 1.0,
                 "history_loads_before": None, "history_loads_after": None,
                 "history_prewarm_verified": None}
        receipt = {"measurement_evidence": {"gate_released": True, "completion_gate_observed": True}}
        for transport in ("rdma", "tcp"):
            with self.subTest(transport=transport):
                job = {"case_id": "case", "case_kind": "media_read", "owner_count": 1,
                       "backend": "tidehunter_nvme_" + transport, "nvmeof_route": "sealed_read",
                       "nvmeof_target": "kernel", "target_io": "direct", "cohort": "sealed_read",
                       "media": {"expected": "cold"}}
                policy = {"target_implementation": "kernel_direct", "target_cache": "direct_file_no_OS_pagecache",
                          "target_cache_verified": True, "selected_transport": transport, "buffered_io": 0}
                targets = [{"target": {"exports": [{"target_receipt": policy}]}}]
                evidence = {"nvmeof": {"available": True, "readers": {"r": {
                    "identity": {"transport": transport},
                    "counters": {"available": True, "counters": counters}}}},
                    "deltas": {"coordinator": {"available": True, "counters": {"read_rpc_count": 0, "hits": 0}}}}
                qualified = cloud_campaign.qualify_nvme_measurement(evidence, 1, transport, targets, "kernel_direct")
                self.assertTrue(qualified["qualified"], qualified["reasons"])
                self.assertTrue(analyze.qualify_sealed_read_accounting(qualified, 100)["qualified"])
                self.assertTrue(analyze.qualify_target_buffered_io(job, targets, 1)["qualified"])
                self.assertEqual(analyze.requested_nvmeof_route({"job": job}), "sealed_read")
                analyze._validate_focused_artifact("media_evidence", media, job, receipt)
                self.assertFalse(cloud_campaign.qualify_nvme_measurement(evidence, 1, transport, targets, "spdk")["qualified"])
                if transport == "tcp":
                    result = cloud_campaign.qualify_nvme_measurement(evidence, 1, transport, targets, "spdk")
                    self.assertIn("expected NVMe target implementation is invalid", result["reasons"])
