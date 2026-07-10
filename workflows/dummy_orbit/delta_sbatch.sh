#!/bin/sh -l
#
# SPHERICAL Dummy Campaign — SLURM batch script (CPU-only, no GPU needed)
#
#SBATCH -A ***-delta-cpu
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --time=00:30:00
#SBATCH --job-name=dummy_orbit
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=END,FAIL

# ── Environment ───────────────────────────────────────────────────────────────
export CM_DIR="/scratch/***/${USER}/campaign_manager"
export ENV_DIR="/u/${USER}/ve/orbit"

unset SLURM_EXPORT_ENV
module load anaconda3 2>/dev/null || true

source "${ENV_DIR}/bin/activate"

# ── Run campaign ──────────────────────────────────────────────────────────────
CAMPAIGN_DIR="${CM_DIR}/workflows/dummy_orbit"
cd "${CAMPAIGN_DIR}"

export RADICAL_ORBIT_BROKER_URL="https://dt-login04.delta.ncsa.illinois.edu:8020"
python run_campaign.py --config config.yaml

echo "=== Orbit campaign done: $(date) ==="

rm -rf asyncflow.session*



# Terminal 1
# token from ~/.radical/orbit/broker.token
#  export RADICAL_ORBIT_BROKER_TOKEN=<token>
#  export RADICAL_ORBIT_BROKER_URL='https://my-broker:8020/'
#  export RADICAL_ORBIT_BROKER_CERT="/u/mgoliyad1/.radical/orbit/broker_cert.pem"
#  export RADICAL_ORBIT_BROKER_KEY="/u/mgoliyad1/.radical/orbit/broker_key.pem"
#  ./bin/radical-orbit-broker.py --port 8020

# Terminal 2
#  salloc -N 1 --time=01:00:00 --account=***-delta-cpu --partition=cpu
#  export RADICAL_ORBIT_BROKER_URL=https://dt-login03.delta.ncsa.illinois.edu:8020
#  dragon -s bin/radical-orbit-endpoint.py --name my-endpoint -p rhapsody
#  or  
#  ./bin/radical-orbit-endpoint.py --name my-endpoint -p rhapsody