"""Tests for inference utils module."""

import json
from unittest.mock import patch

import pytest

from src.inference.utils import ensure_dir, get_gpus_for_node, load_config


class TestLoadConfig:
    def test_load_config_defaults(self, temp_dir):
        config = load_config(str(temp_dir / "nonexistent.yaml"))
        assert "model_path" in config
        assert "num_services" in config
        assert config["num_services"] == 1
        assert config["num_batches"] == 100
        assert config["max_batch_tokens"] == 16000

    def test_load_config_from_file(self, temp_dir):
        config_file = temp_dir / "config.yaml"
        config_file.write_text("num_services: 2\nnum_batches: 50\ncustom_key: custom_value\n")

        config = load_config(str(config_file))

        assert config["num_services"] == 2
        assert config["num_batches"] == 50
        assert config["custom_key"] == "custom_value"
        assert "model_path" in config  # default still present

    def test_load_config_merge_with_defaults(self, temp_dir):
        config_file = temp_dir / "config.yaml"
        config_file.write_text("num_services: 4")

        config = load_config(str(config_file))

        assert config["num_services"] == 4
        assert config["max_batch_tokens"] == 16000

    def test_load_config_all_default_keys_present(self, temp_dir):
        """Every documented default key is present when no file is given."""
        config = load_config(str(temp_dir / "missing.yaml"))
        for key in (
            "model_path",
            "num_services",
            "num_gpus_per_service",
            "num_workers_per_gpu",
            "server_port",
            "max_concurrent",
            "timeout",
            "max_retries",
            "num_batches",
            "max_batch_tokens",
            "use_streaming",
            "output_dir",
            "results_dir",
            "metrics_dir",
            "metrics_file_prefix",
            "metrics_log_interval",
            "debug",
            "engine",
            "SEQUENCES",
        ):
            assert key in config, f"Missing default key: {key}"


class TestEnsureDir:
    def test_ensure_dir_creates_new(self, temp_dir):
        new_dir = temp_dir / "new_directory"
        assert not new_dir.exists()

        result = ensure_dir(new_dir)

        assert new_dir.exists()
        assert new_dir.is_dir()
        assert result == new_dir

    def test_ensure_dir_clears_existing(self, temp_dir):
        existing_dir = temp_dir / "existing"
        existing_dir.mkdir()
        (existing_dir / "file1.txt").write_text("content1")
        (existing_dir / "file2.txt").write_text("content2")
        subdir = existing_dir / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("content3")

        assert len(list(existing_dir.iterdir())) == 3

        result = ensure_dir(existing_dir)

        assert existing_dir.exists()
        assert len(list(existing_dir.iterdir())) == 0
        assert result == existing_dir

    def test_ensure_dir_no_clean_preserves_contents(self, temp_dir):
        """clean=False leaves existing directory contents untouched."""
        existing = temp_dir / "keep"
        existing.mkdir()
        (existing / "file.txt").write_text("preserved")

        result = ensure_dir(existing, clean=False)

        assert (existing / "file.txt").exists()
        assert result == existing

    def test_ensure_dir_nested(self, temp_dir):
        nested_dir = temp_dir / "a" / "b" / "c"
        ensure_dir(nested_dir)
        assert nested_dir.exists()
        assert nested_dir.is_dir()


class TestGetGpusForNode:
    def test_get_devices_cpu_fallback(self):
        """No CUDA available → list of 'cpu' strings with requested count."""
        config = {"num_gpus_per_service": 3}
        with patch("src.inference.utils.detect_device_type", return_value="cpu"):
            devices = get_gpus_for_node(config, node_rank=0)
        assert devices == ["cpu", "cpu", "cpu"]

    def test_get_devices_cuda_when_available(self):
        """CUDA available → cuda:N strings."""
        config = {"num_gpus_per_service": 2}
        with patch("src.inference.utils.detect_device_type", return_value="cuda"):
            with patch("src.inference.utils.get_available_device_count", return_value=4):
                devices = get_gpus_for_node(config, node_rank=0)
        assert devices == ["cuda:0", "cuda:1"]

    def test_get_devices_capped_to_available(self):
        """Requesting more GPUs than available → capped to actual count."""
        config = {"num_gpus_per_service": 4}
        with patch("src.inference.utils.detect_device_type", return_value="cuda"):
            with patch("src.inference.utils.get_available_device_count", return_value=2):
                devices = get_gpus_for_node(config, node_rank=0)
        assert len(devices) == 2
        assert devices == ["cuda:0", "cuda:1"]

    def test_get_devices_single_gpu(self):
        config = {"num_gpus_per_service": 1}
        with patch("src.inference.utils.detect_device_type", return_value="cpu"):
            devices = get_gpus_for_node(config, node_rank=0)
        assert len(devices) == 1


class TestExportMetrics:
    @pytest.mark.asyncio
    async def test_export_creates_json_file(self, temp_dir):
        from src.inference.utils import export_metrics

        output = temp_dir / "metrics" / "out.json"
        metrics = {"requests": 42, "total_tokens": 1000, "errors": 0}
        await export_metrics(output, metrics)
        assert output.exists()
        data = json.loads(output.read_text())
        assert data["requests"] == 42
        assert data["total_tokens"] == 1000

    @pytest.mark.asyncio
    async def test_export_creates_parent_dirs(self, temp_dir):
        from src.inference.utils import export_metrics

        output = temp_dir / "deep" / "nested" / "metrics.json"
        await export_metrics(output, {"x": 1})
        assert output.exists()

    @pytest.mark.asyncio
    async def test_export_roundtrips_data(self, temp_dir):
        from src.inference.utils import export_metrics

        output = temp_dir / "m.json"
        metrics = {"timeseries": [{"t": 1.0, "req_per_sec": 5.2}], "errors": 3}
        await export_metrics(output, metrics)
        data = json.loads(output.read_text())
        assert data["errors"] == 3
        assert data["timeseries"][0]["req_per_sec"] == pytest.approx(5.2)


class TestReadSlurmConfig:
    def test_reads_mapping(self, temp_dir):
        from src.inference.utils import read_slurm_config

        cfg = temp_dir / "slurm.yaml"
        cfg.write_text("nodes: 4\ngpus_per_node: 2\n")
        result = read_slurm_config(cfg)
        assert result["nodes"] == 4
        assert result["gpus_per_node"] == 2

    def test_non_mapping_raises(self, temp_dir):
        from src.inference.utils import read_slurm_config

        cfg = temp_dir / "slurm.yaml"
        cfg.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="mapping"):
            read_slurm_config(cfg)
