#!/bin/bash
export BASE_DIR="${PROJECT}"
export SPHERICAL_DIR="${BASE_DIR}/htp/SPHERICAL"
export WORK_DIR="${SPHERICAL_DIR}/workflows/run_campaign"
export CONDA_ENV="${BASE_DIR}/conda_env"

#mkdir $CONDA_ENV

module load anaconda3


##############################################
# 1. DeepDriveSim base env
##############################################
conda create -y -p $CONDA_ENV/camplaign_manager python=3.9
conda activate $CONDA_ENV/campaing_manager
pip install --upgrade pip setuptools wheel
cd $SPHERICAL_DIR
pip install -e .
cd $WORK_DIR
#pip install -r "requirements.txt"
