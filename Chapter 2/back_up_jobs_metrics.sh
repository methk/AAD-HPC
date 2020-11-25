#!/bin/bash

#SBATCH -N 1
#SBATCH --time=4:30:00
#SBATCH --mem=246000MB
#SBATCH --ntasks-per-node=16
#SBATCH -A try19_EEDL
#SBATCH -p dvd_usr_prod


source $HOME/.bashrc
conda activate base

python $HOME/Scripts/examon_data_reader.py
