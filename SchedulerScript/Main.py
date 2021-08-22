import argparse
import SchedulerToolBox as TB
from pathlib import Path
import os

if __name__ == '__main__':
    # parameter set up
    script_path = '/groups/pupko/alburquerque/RL/Scheduler/PhyloRL/ProjectMain.py'
    queue_name = 'itaym'
    script_exec_command = 'python'
    error_path = Path(os.curdir)/'OUT'
    job_base_name = 'ScJob_'
    ncpus = 1
    out_path = Path(os.curdir)/'OUT'
    conda_env_name = 'MLworkshop'
    conda_env_path = '/groups/itay_mayrose/danaazouri/miniconda3/etc/profile.d/conda.sh'
    data_to_split_into_chunks = [f.name for f in Path('/groups/pupko/alburquerque/RL/Tree_Data/data/All_sized').iterdir() if f.is_dir()]
    data_param_name = 'data_sets'
    chunk_size = 2
    const_script_params = {}
    username = 'alburquerque'
    total_cpu_limit = 100
    total_job_number_limit = 5
    scheduler_job_limit = 3
    job_timeout = None  # a timout for any specific scheduler job in hours. we dont count the minutes

    # task set up
    baseSh = TB.create_base_sh(script_path=script_path, queue_name=queue_name,
                               script_exec_command=script_exec_command, error_path=error_path,
                               job_base_name=job_base_name, ncpus=ncpus, out_path=out_path,
                               conda_env_name=conda_env_name, conda_env_path=conda_env_path)
    taskQueue = TB.create_task_queue(base_sh=baseSh, data_to_split_into_chunks=data_to_split_into_chunks,
                                     data_param_name=data_param_name, chunk_size=chunk_size,
                                     const_script_params=const_script_params)

    # run the scheduling loop
    while not taskQueue.empty():
        freeSlots = TB.get_how_many_free_slots(username=username, total_cpu_limit=total_cpu_limit, cpu_per_job=ncpus,
                                               total_job_number_limit=total_job_number_limit,
                                               scheduler_job_prefix=job_base_name,
                                               scheduler_job_limit=scheduler_job_limit, job_timeout=job_timeout)
        for i in range(freeSlots):
            if taskQueue.empty():
                nextJob = taskQueue.get_nowait()
                TB.schedule_job(nextJob)
            else:
                # were done
                exit(0)

