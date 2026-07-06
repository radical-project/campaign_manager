"""Tests for campaign-manager filesystem and GPU utilities (src.utils.workflow)."""

import json
from unittest.mock import patch

import pytest

from src.utils.workflow import ensure_dir, export_metrics, get_gpus_for_node, read_slurm_config


class TestEnsureDir:
    def test_creates_new(self, temp_dir):
        new_dir = temp_dir / "new_directory"
        assert not new_dir.exists()
        result = ensure_dir(new_dir)
        assert new_dir.exists()
        assert new_dir.is_dir()
        assert result == new_dir

    def test_clears_existing(self, temp_dir):
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

    def test_no_clean_preserves_contents(self, temp_dir):
        existing = temp_dir / "keep"
        existing.mkdir()
        (existing / "file.txt").write_text("preserved")
        result = ensure_dir(existing, clean=False)
        assert (existing / "file.txt").exists()
        assert result == existing

    def test_nested(self, temp_dir):
        nested_dir = temp_dir / "a" / "b" / "c"
        ensure_dir(nested_dir)
        assert nested_dir.exists()
        assert nested_dir.is_dir()


class TestGetGpusForNode:
    def test_cpu_fallback(self):
        config = {"num_gpus_per_service": 3}
        with patch("src.utils.workflow.detect_device_type", return_value="cpu"):
            devices = get_gpus_for_node(config, node_rank=0)
        assert devices == ["cpu", "cpu", "cpu"]

    def test_cuda_when_available(self):
        config = {"num_gpus_per_service": 2}
        with patch("src.utils.workflow.detect_device_type", return_value="cuda"):
            with patch("src.utils.workflow.get_available_device_count", return_value=4):
                devices = get_gpus_for_node(config, node_rank=0)
        assert devices == ["cuda:0", "cuda:1"]

    def test_capped_to_available(self):
        config = {"num_gpus_per_service": 4}
        with patch("src.utils.workflow.detect_device_type", return_value="cuda"):
            with patch("src.utils.workflow.get_available_device_count", return_value=2):
                devices = get_gpus_for_node(config, node_rank=0)
        assert devices == ["cuda:0", "cuda:1"]

    def test_single_gpu(self):
        config = {"num_gpus_per_service": 1}
        with patch("src.utils.workflow.detect_device_type", return_value="cpu"):
            devices = get_gpus_for_node(config, node_rank=0)
        assert len(devices) == 1


class TestExportMetrics:
    @pytest.mark.asyncio
    async def test_creates_json_file(self, temp_dir):
        output = temp_dir / "metrics" / "out.json"
        metrics = {"requests": 42, "total_tokens": 1000, "errors": 0}
        await export_metrics(output, metrics)
        assert output.exists()
        data = json.loads(output.read_text())
        assert data["requests"] == 42

    @pytest.mark.asyncio
    async def test_creates_parent_dirs(self, temp_dir):
        output = temp_dir / "deep" / "nested" / "metrics.json"
        await export_metrics(output, {"x": 1})
        assert output.exists()

    @pytest.mark.asyncio
    async def test_roundtrips_data(self, temp_dir):
        output = temp_dir / "m.json"
        metrics = {"timeseries": [{"t": 1.0, "req_per_sec": 5.2}], "errors": 3}
        await export_metrics(output, metrics)
        data = json.loads(output.read_text())
        assert data["errors"] == 3
        assert data["timeseries"][0]["req_per_sec"] == pytest.approx(5.2)


class TestReadSlurmConfig:
    def test_reads_mapping(self, temp_dir):
        cfg = temp_dir / "slurm.yaml"
        cfg.write_text("nodes: 4\ngpus_per_node: 2\n")
        result = read_slurm_config(cfg)
        assert result["nodes"] == 4
        assert result["gpus_per_node"] == 2

    def test_non_mapping_raises(self, temp_dir):
        cfg = temp_dir / "slurm.yaml"
        cfg.write_text("- item1\n- item2\n")
        with pytest.raises(ValueError, match="mapping"):
            read_slurm_config(cfg)
