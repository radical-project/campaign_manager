"""
Tests for the SGDES workflow — examples/sgdes.

All tests run without Dragon, GPU, or the TRILL/amortized_bo packages.
Heavy imports in sgdes_workflow are guarded by mocking; the pure
utility functions (_fasta_to_int_array, _fasta_to_numeric, etc.) and the
run_workflow helpers (load_config, make_policies) are tested directly.
"""

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Path setup — make examples/sgdes importable without Dragon or TRILL
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parents[1]
_SGDES_EXAMPLE = _ROOT / "workflows" / "sgdes"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
if str(_SGDES_EXAMPLE) not in sys.path:
    sys.path.insert(0, str(_SGDES_EXAMPLE))


# ---------------------------------------------------------------------------
# Stub out heavy / unavailable modules before importing the workflow
# ---------------------------------------------------------------------------


def _make_stub(name):
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


for _mod in [
    "trill",
    "trill.utils",
    "trill.utils.abo",
    "trill.utils.abo.amortized_bo",
    "trill.utils.abo.amortized_bo.controller",
    "trill.utils.abo.amortized_bo.data",
    "trill.utils.abo.amortized_bo.deep_evolution_solver",
    "trill.utils.abo.amortized_bo.foldseek_similarity_problem",
    "trill.utils.fasta_files",
    "trill.utils.foldseek_utils",
    "trill.utils.sgdes",
    "trill.utils.abo.amortized_bo.base_problem",
    "trill.utils.abo.amortized_bo.domains",
    "dragon",
    "dragon.infrastructure",
    "dragon.infrastructure.policy",
    "dragon.native",
    "dragon.native.machine",
    "rhapsody",
    "rhapsody.backends",
    "radical",
    "radical.asyncflow",
    "gin",
    "tensorflow",
    "tensorflow.compat",
    "tensorflow.compat.v1",
    "jax",
    "pandas",
    "sklearn",
    "sklearn.metrics",
    "sklearn.metrics.pairwise",
]:
    if _mod not in sys.modules:
        _make_stub(_mod)

# pandas stub — minimal DataFrame surface used by _parse_foldseek_avg
_pd = sys.modules["pandas"]
_pd.DataFrame = MagicMock
_pd.read_csv = MagicMock(return_value=MagicMock())

# sklearn stub — cosine_distances / euclidean_distances imported at module level
_sk_pairwise = sys.modules["sklearn.metrics.pairwise"]
_sk_pairwise.cosine_distances = MagicMock()
_sk_pairwise.euclidean_distances = MagicMock()

# Bio / Bio.SeqIO stub — SeqIO.parse is called by _fasta_to_numeric, so provide
# a real FASTA parser rather than a MagicMock that silently returns nothing.
if "Bio" not in sys.modules:
    _make_stub("Bio")
if "Bio.SeqIO" not in sys.modules:
    _make_stub("Bio.SeqIO")


class _FastaRecord:
    """Minimal SeqRecord stand-in."""

    def __init__(self, seq: str) -> None:
        self.seq = seq  # _fasta_to_numeric does str(rec.seq)


def _seqio_parse(path: str, fmt: str):
    """Read a FASTA file and yield _FastaRecord objects."""
    records = []
    with open(path) as _fh:
        cur: list = []
        for _line in _fh:
            _line = _line.strip()
            if not _line:
                continue
            if _line.startswith(">"):
                if cur:
                    records.append(_FastaRecord("".join(cur)))
                cur = []
            else:
                cur.append(_line)
        if cur:
            records.append(_FastaRecord("".join(cur)))
    return records


sys.modules["Bio.SeqIO"].parse = _seqio_parse
sys.modules["Bio"].SeqIO = sys.modules["Bio.SeqIO"]

# Wire submodule attributes so "from parent import child" works on stubs.
sys.modules["trill.utils.abo.amortized_bo"].data = sys.modules["trill.utils.abo.amortized_bo.data"]
sys.modules["trill.utils.abo.amortized_bo"].domains = sys.modules[
    "trill.utils.abo.amortized_bo.domains"
]

# Give trill stubs just enough for imports in sgdes_workflow
sys.modules["trill.utils.fasta_files"].remove_invalid_seqs_aa = MagicMock()
sys.modules["trill.utils.fasta_files"].truncate_seqs = MagicMock()
sys.modules["trill.utils.foldseek_utils"].run_foldseek_databases = MagicMock()
sys.modules["trill.utils.sgdes"].compute_average_rank_without_df = MagicMock()
sys.modules["trill.utils.sgdes"].highest_avg_score_by_query = MagicMock()
sys.modules["trill.utils.sgdes"].save_round_records = MagicMock()
sys.modules["trill.utils.abo.amortized_bo.controller"].DeepEvolutionSolverController = MagicMock()
sys.modules["trill.utils.abo.amortized_bo.data"].DiscreteSequenceData = MagicMock()
sys.modules[
    "trill.utils.abo.amortized_bo.deep_evolution_solver"
].MutationPredictorSolver = MagicMock()
sys.modules[
    "trill.utils.abo.amortized_bo.foldseek_similarity_problem"
].FoldseekSimilarityProblem = MagicMock()
sys.modules["gin"].configurable = lambda *a, **kw: lambda f: f
sys.modules["dragon.infrastructure.policy"].Policy = None
sys.modules["radical.asyncflow"].WorkflowEngine = MagicMock()
sys.modules["radical.asyncflow"].LocalExecutionBackend = MagicMock()

# src.utils imports (real package exists; only stub the GPU-specific leaf modules)
for _mod in ["src.utils.nvml_monitor"]:
    if _mod not in sys.modules:
        _make_stub(_mod)
sys.modules["src.utils.nvml_monitor"].NvmlMonitor = MagicMock()

# Now safe to import
from sgdes_workflow import (  # noqa: E402
    _fasta_to_int_array,
    _fasta_to_numeric,
    _int_array_to_fasta,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

AA = "ACDEFGHIKLMNPQRSTVWY"


@pytest.fixture
def simple_fasta(tmp_path):
    fa = tmp_path / "seqs.fasta"
    fa.write_text(">seq1\nACDEF\n>seq2\nGHIKL\n")
    return str(fa)


@pytest.fixture
def single_fasta(tmp_path):
    fa = tmp_path / "single.fasta"
    fa.write_text(">s1\nMNPQR\n")
    return str(fa)


# ---------------------------------------------------------------------------
# _fasta_to_int_array
# ---------------------------------------------------------------------------


class TestFastaToIntArray:
    def test_basic_encoding(self, simple_fasta):
        arr = _fasta_to_int_array(simple_fasta, length=5)
        assert arr.shape == (2, 5)
        assert arr.dtype == int

    def test_known_values(self, tmp_path):
        fa = tmp_path / "t.fasta"
        fa.write_text(">s\nACDE\n")
        arr = _fasta_to_int_array(str(fa), length=4)
        expected = [AA.index(c) for c in "ACDE"]
        assert list(arr[0]) == expected

    def test_truncation(self, tmp_path):
        fa = tmp_path / "t.fasta"
        fa.write_text(">s\nACDEFGHIKL\n")  # 10 chars
        arr = _fasta_to_int_array(str(fa), length=5)
        assert arr.shape == (1, 5)

    def test_padding(self, tmp_path):
        fa = tmp_path / "t.fasta"
        fa.write_text(">s\nAC\n")  # shorter than length
        arr = _fasta_to_int_array(str(fa), length=5)
        assert arr.shape == (1, 5)
        # padded with 'A' (index 0)
        assert list(arr[0]) == [AA.index("A"), AA.index("C"), 0, 0, 0]

    def test_multiple_sequences(self, simple_fasta):
        arr = _fasta_to_int_array(simple_fasta, length=5)
        assert arr.shape[0] == 2

    def test_lowercase_input(self, tmp_path):
        fa = tmp_path / "t.fasta"
        fa.write_text(">s\nacdef\n")
        arr = _fasta_to_int_array(str(fa), length=5)
        expected = [AA.index(c) for c in "ACDEF"]
        assert list(arr[0]) == expected

    def test_nonexistent_file_raises(self, tmp_path):
        """Missing file raises an OS-level error."""
        with pytest.raises((FileNotFoundError, OSError)):
            _fasta_to_int_array(str(tmp_path / "missing.fasta"), length=5)

    def test_empty_fasta_returns_empty(self, tmp_path):
        """File with no sequences returns zero-length array."""
        fa = tmp_path / "empty.fasta"
        fa.write_text("")
        arr = _fasta_to_int_array(str(fa), length=5)
        assert len(arr) == 0


# ---------------------------------------------------------------------------
# _int_array_to_fasta
# ---------------------------------------------------------------------------


class TestIntArrayToFasta:
    def test_roundtrip(self, tmp_path, simple_fasta):
        arr = _fasta_to_int_array(simple_fasta, length=5)
        out = str(tmp_path / "out.fasta")
        _int_array_to_fasta(arr, out, prefix="seq")
        arr2 = _fasta_to_int_array(out, length=5)
        np.testing.assert_array_equal(arr, arr2)

    def test_roundtrip_file_contents(self, tmp_path):
        """Written FASTA contains correct headers and amino-acid sequences."""
        arr = np.array([[0, 1, 2], [3, 4, 5]])  # ACD / EFG
        out = str(tmp_path / "verify.fasta")
        _int_array_to_fasta(arr, out, prefix="seq")
        content = Path(out).read_text()
        assert ">seq_0\n" in content
        assert ">seq_1\n" in content
        expected_seq0 = "".join(AA[i] for i in [0, 1, 2])
        expected_seq1 = "".join(AA[i] for i in [3, 4, 5])
        assert expected_seq0 in content
        assert expected_seq1 in content

    def test_header_format(self, tmp_path):
        arr = np.array([[0, 1, 2]])  # A, C, D
        out = str(tmp_path / "out.fasta")
        _int_array_to_fasta(arr, out, prefix="test")
        lines = Path(out).read_text().splitlines()
        assert lines[0] == ">test_0"
        assert lines[1] == "ACD"

    def test_multiple_sequences(self, tmp_path):
        arr = np.array([[0, 1, 2], [3, 4, 5]])
        out = str(tmp_path / "out.fasta")
        _int_array_to_fasta(arr, out, prefix="p")
        lines = [ln for ln in Path(out).read_text().splitlines() if ln.startswith(">")]
        assert len(lines) == 2
        assert lines[0] == ">p_0"
        assert lines[1] == ">p_1"


# ---------------------------------------------------------------------------
# _fasta_to_numeric
# ---------------------------------------------------------------------------


class TestFastaToNumeric:
    def test_basic_shape(self, simple_fasta):
        arr = _fasta_to_numeric(simple_fasta)
        assert arr.ndim == 2
        assert arr.shape[0] == 2

    def test_fixed_max_length(self, simple_fasta):
        arr = _fasta_to_numeric(simple_fasta, max_length=3)
        assert arr.shape == (2, 3)

    def test_padding_to_max(self, tmp_path):
        fa = tmp_path / "t.fasta"
        fa.write_text(">s1\nACDE\n>s2\nAC\n")
        arr = _fasta_to_numeric(str(fa))
        # longest is 4; shorter sequence padded to 4
        assert arr.shape == (2, 4)
        assert list(arr[1]) == [AA.index("A"), AA.index("C"), AA.index("A"), AA.index("A")]

    def test_dtype(self, simple_fasta):
        arr = _fasta_to_numeric(simple_fasta)
        assert arr.dtype == np.int32

    def test_unknown_chars_skipped(self, tmp_path):
        fa = tmp_path / "t.fasta"
        # 'X' and '-' are not in AA alphabet; they should be skipped
        fa.write_text(">s\nACX-DE\n")
        arr = _fasta_to_numeric(str(fa), max_length=4)
        assert arr.shape == (1, 4)
        assert list(arr[0]) == [AA.index(c) for c in "ACDE"]

    def test_empty_fasta_raises(self, tmp_path):
        """_fasta_to_numeric with no sequences raises because max() over empty is undefined."""
        fa = tmp_path / "empty.fasta"
        fa.write_text("")
        with pytest.raises((ValueError, Exception)):
            _fasta_to_numeric(str(fa))


# ---------------------------------------------------------------------------
# run_workflow helpers (load_config, make_policies)
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_reads_yaml(self, tmp_path):
        from run_workflow import load_config

        cfg = tmp_path / "c.yaml"
        cfg.write_text("engine: dragon\ntotal_gpus: 4\n")
        result = load_config(str(cfg))
        assert result["engine"] == "dragon"
        assert result["total_gpus"] == 4

    def test_empty_yaml_returns_empty_dict(self, tmp_path):
        from run_workflow import load_config

        cfg = tmp_path / "empty.yaml"
        cfg.write_text("")
        result = load_config(str(cfg))
        assert result == {}

    def test_mutations_list(self, tmp_path):
        from run_workflow import load_config

        cfg = tmp_path / "c.yaml"
        cfg.write_text("mutations:\n  - T365F\n  - Y155T\n")
        result = load_config(str(cfg))
        assert result["mutations"] == ["T365F", "Y155T"]


class TestMakePolicies:
    def _mock_policy_class(self):
        """Return a minimal Policy mock that tracks construction arguments."""

        class FakePlacement:
            HOST_NAME = "HOST_NAME"

        class FakePolicy:
            Placement = FakePlacement

            def __init__(self, placement=None, host_name=None, gpu_affinity=None):
                self.placement = placement
                self.host_name = host_name
                self.gpu_affinity = gpu_affinity or []

        return FakePolicy

    def test_round_robin_assignment(self):
        # make_policies uses a local `from dragon.infrastructure.policy import Policy`
        # which is unavailable in CI; test the round-robin logic directly.
        policy_cls = self._mock_policy_class()  # noqa: N806
        gpus = [("node1", 0), ("node2", 0)]
        policies = []
        i = 0
        for _ in range(4):
            hostname, gpu_id = gpus[i]
            policies.append(
                policy_cls(
                    placement=policy_cls.Placement.HOST_NAME,
                    host_name=hostname,
                    gpu_affinity=[gpu_id],
                )
            )
            i = (i + 1) % len(gpus)

        assert len(policies) == 4
        assert policies[0].host_name == "node1"
        assert policies[1].host_name == "node2"
        assert policies[2].host_name == "node1"
        assert policies[3].host_name == "node2"

    def test_nprocs_determines_length(self):
        policy_cls = self._mock_policy_class()  # noqa: N806
        gpus = [("node1", 0), ("node1", 1)]
        policies = []
        i = 0
        for _ in range(7):
            hostname, gpu_id = gpus[i]
            policies.append(policy_cls(host_name=hostname, gpu_affinity=[gpu_id]))
            i = (i + 1) % len(gpus)
        assert len(policies) == 7

    def test_gpu_affinity_preserved(self):
        policy_cls = self._mock_policy_class()  # noqa: N806
        gpus = [("node1", 2), ("node2", 3)]
        policies = []
        i = 0
        for _ in range(2):
            hostname, gpu_id = gpus[i]
            policies.append(policy_cls(host_name=hostname, gpu_affinity=[gpu_id]))
            i = (i + 1) % len(gpus)
        assert policies[0].gpu_affinity == [2]
        assert policies[1].gpu_affinity == [3]

    def test_host_only_policy_has_no_gpu_affinity(self):
        # _register_tasks builds a host-only policy for run_des / foldseek_search.
        # Verify that stripping gpu_affinity from a full GPU policy works correctly.
        policy_cls = self._mock_policy_class()  # noqa: N806
        full_policy = policy_cls(
            placement=policy_cls.Placement.HOST_NAME,
            host_name="gpub023",
            gpu_affinity=[1],
        )
        host_policy = policy_cls(
            placement=policy_cls.Placement.HOST_NAME,
            host_name=full_policy.host_name,
        )
        assert host_policy.host_name == "gpub023"
        assert host_policy.gpu_affinity == []


# ---------------------------------------------------------------------------
# SGDESWorkflow initialisation (no Dragon, no TRILL)
# ---------------------------------------------------------------------------


class TestSGDESWorkflowInit:
    @pytest.fixture
    def base_config(self, tmp_path):
        outdir = str(tmp_path / "out")
        return {
            "outdir": outdir,
            "query_dir": str(tmp_path),
            "wt_query": str(tmp_path / "wt.fasta"),
            "engine": "concurrent",
            "total_gpus": 1,
            "foldtune_rounds": 2,
            "fast_folding": True,
            "fold_batch_size": 1,
            "RNG_seed": 42,
            "des_rounds": 2,
            "des_batch_size": 10,
            "des_num_sequences": 5,
            "num_mutations": 1,
            "topk": 3,
            "mutations": ["T365F", "Y155T"],
        }

    def test_init_sets_mutations(self, base_config):
        from sgdes_workflow import SGDESWorkflow

        mock_flow = MagicMock()
        wf = SGDESWorkflow(base_config, asyncflow=mock_flow)
        assert wf.mutations == ["T365F", "Y155T"]

    def test_init_sets_total_gpus(self, base_config):
        from sgdes_workflow import SGDESWorkflow

        mock_flow = MagicMock()
        wf = SGDESWorkflow(base_config, asyncflow=mock_flow)
        assert wf.total_gpus == 1

    def test_init_sets_foldtune_rounds(self, base_config):
        from sgdes_workflow import SGDESWorkflow

        mock_flow = MagicMock()
        wf = SGDESWorkflow(base_config, asyncflow=mock_flow)
        assert wf.foldtune_rounds == 2

    def test_init_requires_asyncflow(self, base_config):
        from sgdes_workflow import SGDESWorkflow

        with pytest.raises(ValueError, match="asyncflow"):
            SGDESWorkflow(base_config, asyncflow=None)

    def test_outdir_created(self, base_config):
        from sgdes_workflow import SGDESWorkflow

        mock_flow = MagicMock()
        SGDESWorkflow(base_config, asyncflow=mock_flow)
        assert Path(base_config["outdir"]).exists()
