#!/bin/sh -l

#SBATCH -A ***
#SBATCH --partition=GPU-shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#xSBATCH --gpus=v100-32:16
#SBATCH --gpus=4
#SBATCH --time=00:25:00
#SBATCH --job-name=spher
#SBATCH --mail-user=mariya.goliyad@rutgers.edu
#SBATCH --mail-type=ALL

module load cuda
module load gcc
module load anaconda3
conda activate $PROJECT/conda_env/test_inf

cd $PROJECT/htp/SPHERICAL/workflows/esm2_inference

# dragon-network-config --output-to-yaml 

# dragon -w ssh --network-config slurm.yaml run_esm2_infern.py --config_file config.yaml

python run_esm2_infern.py