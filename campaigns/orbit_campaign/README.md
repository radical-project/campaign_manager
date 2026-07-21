# Orbit Campaign

Two-stage search → refine pipeline that submits `dummy_workflow/simulation.py`
tasks to a remote HPC compute node via ORBIT + rhapsody.  Each replica fans out
`num_sims` independent tasks; orbit runs them all in parallel so per-replica
wall time ≈ one simulation's duration (~15 s) regardless of `num_sims`.

## Prerequisites

You need three things running before launching the campaign:

1. **TLS credentials** (one-time setup — already in place):
   ```bash
   export RADICAL_ORBIT_BROKER_CERT="/u/$USER/.radical/orbit/broker_cert.pem"
   export RADICAL_ORBIT_BROKER_KEY="/u/$USER/.radical/orbit/broker_key.pem"
   export RADICAL_ORBIT_BROKER_TOKEN='<token>'
   ```

2. **Broker** — runs on a login node (Terminal 1):
   ```bash
   source ~/ve/orbit_campaign/bin/activate
   cd $SCRATCH/$USER/radical.orbit
   ./bin/radical-orbit-broker.py --port 8020
   ```
   Note the printed URL, e.g. `https://dt-login03.delta.ncsa.illinois.edu:8020`.

3. **Endpoint** — runs on a compute node inside an allocation (Terminal 2):
   ```bash
   # Get an allocation first
   salloc -N 1 --time=01:00:00 --account=$SBATCH_ACCOUNT --partition=cpu

   # SSH into the compute node
   ssh <compute-node>

   export RADICAL_ORBIT_BROKER_URL=https://dt-login03.delta.ncsa.illinois.edu:8020
   source ~/ve/orbit_campaign/bin/activate
   cd $SCRATCH/$USER/radical.orbit
   ./bin/radical-orbit-endpoint.py --name my-endpoint -p rhapsody
   ```

## Running the Campaign

With broker and endpoint running, submit from the login node (Terminal 3 or via SLURM):

```bash
cd $SCRATCH/$USER/campaign_manager/campaigns/orbit_campaign
source ~/ve/orbit_campaign/bin/activate
export RADICAL_ORBIT_BROKER_URL=https://dt-login03.delta.ncsa.illinois.edu:8020

python run_campaign.py --config config.yaml
```

Or submit via SLURM (sets the broker URL via the environment before calling `run_campaign.py`):
```bash
export RADICAL_ORBIT_BROKER_URL=https://dt-login03.delta.ncsa.illinois.edu:8020
sbatch delta_sbatch.sh
```

## Config

Key parameters in `config.yaml`:

| Key | Description |
|-----|-------------|
| `python_exe` | Python interpreter on the compute node (`/u/$USER/ve/orbit_campaign/bin/python`) |
| `work_dir` | Directory containing `simulation.py` on the shared filesystem |
| `sim_output_dir` | Shared scratch directory where `.npz` output files are written |
| `num_sims` | Simulations submitted per replica; all run in parallel on the endpoint |
| `sim_batch_size` | HTTP batch size for task submission (keep ≤ `num_sims`) |
| `refine_threshold` | Score below which a search replica triggers a refine run |
| `campaign_target` | Number of refine replicas to complete before stopping |

Keep `concurrency_cap × num_sims ≤ ~10` to avoid overwhelming the endpoint with
concurrent Python subprocess forks.

## How It Works

- **search**: each replica submits `num_sims` independent `simulation.py` tasks
  to the orbit endpoint.  Orbit runs them all in parallel (~15 s total).  After
  all tasks land, the campaign manager reads the `.npz` files and computes
  `mean(|y|)` as the score.
- **refine**: triggered when `score < refine_threshold`; runs another batch of
  simulations in a separate output directory.
- The broker and endpoint can be reused across multiple campaign runs as long
  as they stay connected.
