import time
import pathlib
from pathlib import Path
import SchedulerToolBox as TB


if __name__ == '__main__':
    # JOB parameter set up

    # path to the script you want to run
    script_path = '/groups/pupko/alburquerque/RL/Scheduler/ProjectMain.py'
    # name of the queue to submit jobs to
    queue_name = 'itaym'
    # exec command for the script
    script_exec_command = 'python'
    # path to error and out folder - make sure this folder exists
    error_path = pathlib.Path().resolve() / 'OUT'
    out_path = pathlib.Path().resolve() / 'OUT'
    # job name prefix
    job_base_name = 'ScJob_'
    # number of cpus for each scheduled job
    ncpus = 20
    # name and path of your conda env - set to None if no conda env is required
    conda_env_name = 'MLworkshop'
    conda_env_path = '/groups/itay_mayrose/danaazouri/miniconda3/etc/profile.d/conda.sh'
    # additional commands text to run - for example if you want to load a python module instead of a cond env
    additional_commands = ''

    # SCRIPT params given to your script in --{param_name} {param_value} format
    # the data to split into chunks, each chunk will be given to the job in list form
    data_to_split_into_chunks = [f.name for f in Path('/groups/pupko/alburquerque/RL/Tree_Data/data/All_sized').iterdir() if f.is_dir()]
    # chunk size for each job
    chunk_size = 1
    # name of the data param you want to give your script
    data_param_name = 'data'
    # additionl params to give to your script. dictionary keys are the param names dictionary values are the values
    const_script_params = {'cpus': 20}
    # your username
    username = 'alburquerque'

    # limits on the scheduler
    # total cpu limit of all running jobs
    total_cpu_limit = 100
    # total number of jobs allowed to run (not including the scheduler itself)
    total_job_number_limit = 7
    # total number of jobs the scheduler is allowed to run in parallel
    scheduler_job_limit = 5
    # a timout for any specific scheduler job in hours. we dont count the minutes
    job_timeout = 1
    # the time between checks in secs, currently set to ten minutes
    time_between_check_in_sec = 600

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
            if not taskQueue.empty():
                nextJob = taskQueue.get_nowait()
                TB.schedule_job(nextJob)
            else:
                # were done
                exit(0)
        time.sleep(time_between_check_in_sec)
