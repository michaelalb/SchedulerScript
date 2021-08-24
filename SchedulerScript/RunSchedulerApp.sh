
#!/bin/bash

#PBS -S /bin/bash
#PBS -r y
#PBS -q {your preferred queue}
#PBS -v PBS_O_SHELL=bash,PBS_ENVIRONMENT=PBS_BATCH
#PBS -N SCLoopScheduler
#PBS -e {error file path for the scheduling job}
#PBS -o {out file path for the scheduling job}

source #{path to your conde env}
conda activate # name of your conda env
cd # the scripts home base
PYTHONPATH=$(pwd)

python #{path to your scripts "Main.py" file}