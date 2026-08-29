"""Tests for src/campaign/gpu.py — graceful fallback paths."""

import logging
from unittest.mock import MagicMock, patch

from src.campaign.gpu import detect_gpus, find_gpus, make_policies

# ---------------------------------------------------------------------------
# detect_gpus
# ---------------------------------------------------------------------------


class TestDetectGpus:
    def test_torch_available_returns_device_count(self):
        fake_torch = MagicMock()
        fake_torch.cuda.device_count.return_value = 3
        with patch.dict("sys.modules", {"torch": fake_torch}):
            assert detect_gpus() == 3

    def test_torch_raises_falls_back_to_nvidia_smi(self):
        fake_torch = MagicMock()
        fake_torch.cuda.device_count.side_effect = RuntimeError("no cuda")
        smi_output = "0\n1\n2\n"
        with patch.dict("sys.modules", {"torch": fake_torch}):
            with patch("subprocess.check_output", return_value=smi_output):
                assert detect_gpus() == 3

    def test_torch_import_error_falls_back_to_nvidia_smi(self):
        with patch.dict("sys.modules", {"torch": None}):
            smi_output = "0\n1\n"
            with patch("subprocess.check_output", return_value=smi_output):
                assert detect_gpus() == 2

    def test_both_fail_returns_zero(self):
        with patch.dict("sys.modules", {"torch": None}):
            with patch("subprocess.check_output", side_effect=FileNotFoundError):
                assert detect_gpus() == 0

    def test_nvidia_smi_empty_output_returns_zero(self):
        with patch.dict("sys.modules", {"torch": None}):
            with patch("subprocess.check_output", return_value=""):
                assert detect_gpus() == 0


# ---------------------------------------------------------------------------
# find_gpus
# ---------------------------------------------------------------------------


class TestFindGpus:
    def test_dragon_not_available_returns_empty_list(self):
        with patch.dict("sys.modules", {"dragon": None, "dragon.native": None,
                                        "dragon.native.machine": None}):
            assert find_gpus() == []

    def test_dragon_available_returns_gpu_tuples(self):
        fake_node_a = MagicMock()
        fake_node_a.hostname = "host1"
        fake_node_a.gpus = [0, 1]

        fake_node_b = MagicMock()
        fake_node_b.hostname = "host2"
        fake_node_b.gpus = [0]

        fake_system = MagicMock()
        fake_system.return_value.nodes = ["huid_a", "huid_b"]

        fake_node_cls = MagicMock(side_effect=[fake_node_a, fake_node_b])

        fake_machine = MagicMock()
        fake_machine.Node = fake_node_cls
        fake_machine.System = fake_system

        with patch.dict("sys.modules", {
            "dragon": MagicMock(),
            "dragon.native": MagicMock(),
            "dragon.native.machine": fake_machine,
        }):
            result = find_gpus()

        assert ("host1", 0) in result
        assert ("host1", 1) in result
        assert ("host2", 0) in result

    def test_dragon_raises_returns_empty_list(self):
        bad_machine = MagicMock()
        bad_machine.System.side_effect = RuntimeError("cluster unavailable")

        with patch.dict("sys.modules", {
            "dragon": MagicMock(),
            "dragon.native": MagicMock(),
            "dragon.native.machine": bad_machine,
        }):
            assert find_gpus() == []


# ---------------------------------------------------------------------------
# make_policies
# ---------------------------------------------------------------------------


class TestMakePolicies:
    def test_empty_gpu_ids_returns_empty(self):
        assert make_policies([("host1", 0)], []) == []

    def test_empty_gpu_pool_returns_empty(self):
        assert make_policies([], [0, 1]) == []

    def test_dragon_not_installed_returns_empty(self):
        with patch.dict("sys.modules", {
            "dragon": None,
            "dragon.infrastructure": None,
            "dragon.infrastructure.policy": None,
        }):
            result = make_policies([("host1", 0)], [0])
        assert result == []

    def test_dragon_available_returns_one_policy(self):
        fake_policy_instance = MagicMock()
        fake_policy_cls = MagicMock(return_value=fake_policy_instance)
        fake_policy_cls.Placement.HOST_NAME = "HOST_NAME"

        fake_infra = MagicMock()
        fake_infra.policy.Policy = fake_policy_cls

        with patch.dict("sys.modules", {
            "dragon": MagicMock(),
            "dragon.infrastructure": fake_infra,
            "dragon.infrastructure.policy": fake_infra.policy,
        }):
            result = make_policies([("host1", 0), ("host1", 1)], [0, 1])

        assert result == [fake_policy_instance]
        fake_policy_cls.assert_called_once_with(
            placement="HOST_NAME",
            host_name="host1",
            gpu_affinity=[0, 1],
        )

    def test_policy_construction_failure_logs_warning_and_returns_empty(self, caplog):
        fake_policy_cls = MagicMock(side_effect=TypeError("bad arg"))
        fake_policy_cls.Placement.HOST_NAME = "HOST_NAME"

        fake_infra_policy = MagicMock()
        fake_infra_policy.Policy = fake_policy_cls

        with patch.dict("sys.modules", {
            "dragon": MagicMock(),
            "dragon.infrastructure": MagicMock(),
            "dragon.infrastructure.policy": fake_infra_policy,
        }):
            with caplog.at_level(logging.WARNING, logger="src.campaign.gpu"):
                result = make_policies([("host1", 0)], [0])

        assert result == []
        assert any("make_policies failed" in r.message for r in caplog.records)
