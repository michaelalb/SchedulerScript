import math
import queue
import pathlib
from functools import partial
import Consts
import numpy as np
import subprocess
import re
import pandas as pd
import os


def create_base_sh(script_path, queue_name, script_exec_command, error_path, job_base_name, ncpus, out_path,
                   conda_env_name, conda_env_path):
    """
    this function is basically a wrapper for the string format function, were using partial function because later
    we want to format some more into the template SH file and format is mean so when evaluating the function it requires
    all params.
    :param script_path: path to the script you want to run
    :param queue_name: the queue name for jobs to be submitted in
    :param script_exec_command: the command that executes the script, for python scripts "python"
    :param error_path: path to error files for the job (make sure it exists)
    :param job_base_name: the base name for the scheduled jobs - will be suffixed with "_jobNumber"
    :param ncpus: number of cpus to allocate to each job
    :param out_path: path to output files for the job (make sure it exists)
    :param conda_env_name: optional param - name of your conda environment
    :param conda_env_path: optional param - path to your conda environment
    :return: a partial formatted function representation the partially formatted bash text
    """
    if conda_env_name is not None and conda_env_path is not None:
        conda_text = f'source {conda_env_path} \n conda activate {conda_env_name}'
    else:
        conda_text = ''
    return partial(Consts.ShBaseText.format, ncpus=ncpus, queue=queue_name, job_base_name=job_base_name,
                   error_path=str(error_path), out_path=str(out_path), conda=conda_text,
                   script_exe_command=script_exec_command, script_path=script_path)


def split_data_sets_into_chunks(data_params, chunk_size=2):
    """
    this function splits all data params into chunks (each chunk will go to a job)
    :param data_params: list of data parmas
    :param chunk_size: the size of each chunk
    :return: a list of lists, each internal list is a set of data params to the jobs
    """
    assert type(data_params) in [list, np.array, np.ndarray]
    num_of_chunks = math.ceil(len(data_params) / chunk_size)
    all_data_sets_in_chunks = np.array_split(data_params, num_of_chunks)
    return all_data_sets_in_chunks


def create_task_queue(const_script_params, data_to_split_into_chunks, data_param_name, chunk_size, base_sh):
    """
     this function creates all tasks to be executed upon once for the entire life cycle of the scheduler
    :param const_script_params: constant parameters (the same for every job)
     to be passed to the script in a -- format. the params must come in a dictionary
    format where the keys are the param names and the values are the param values
    :param data_to_split_into_chunks: this is the whole chuck of data params that you want your script to get in chunks
    :param data_param_name: the name of the data param
    :param chunk_size: the chunk size of data params
    :param base_sh: base for sh script
    :return: a queue full of tasks (actually SH file texts)
    """
    # set up base
    taskQueue = queue.Queue()
    dataParamsChunks = split_data_sets_into_chunks(data_params=data_to_split_into_chunks, chunk_size=chunk_size)
    baseParamText = ''
    for paramName, paramValue in const_script_params.items():
        baseParamText += f' --{str(paramName)} {str(paramValue)}'

    # create tasks
    jobNumber = 0
    for dataParamChunk in dataParamsChunks:
        jobNumber += 1
        paramText = baseParamText + f' --{data_param_name} {str(list(dataParamChunk))}'
        currentJobSh = base_sh(job_unique_id=jobNumber, script_param_text=paramText)
        taskQueue.put(currentJobSh, block=True)

    return taskQueue


def get_user_job_stats(username, scheduler_job_prefix):
    """
    gets the users current job statistics (running and queued) and parses them
    :param username: the username of the submitting user
    :param scheduler_job_prefix: the naming prefix of each job
    :return: a data frame of all current jobs and a data frame of current jobs made by the scheduler
    """
    result = subprocess.run(['qstat', f'-u {username}'], stdout=subprocess.PIPE)
    result_lines = (str(result.stdout).split('\\n'))[5:-1]  # irrelevant text from qstat
    results_params = [re.sub('\s+', ' ', x).split(' ') for x in result_lines]  # remove spaces and turn to data
    results_df = pd.DataFrame(results_params, columns=Consts.QstatDataColumns)
    results_df = results_df[~results_df['job_name'].str.contains('SCLoop')]  # ignore the scheduling job itself
    results_df['cpus'] = results_df['cpus'].astype(int)

    scheduler_jobs = results_df[results_df['job_name'].str.startswith(scheduler_job_prefix)]
    scheduler_jobs['elapsed_time'] = scheduler_jobs['elapsed_time'].astype(str)
    scheduler_jobs['elapsed_time'] = scheduler_jobs['elapsed_time'].str.split(':')  # just care about the hours
    scheduler_jobs['elapsed_time'] = scheduler_jobs['elapsed_time'].apply(lambda x: int(x[0]))

    return results_df, scheduler_jobs


def get_how_many_free_slots(username, total_cpu_limit, total_job_number_limit, scheduler_job_limit, job_timeout,
                            scheduler_job_prefix, cpu_per_job):
    """
    calculated how many free slots there are for the scheduler to submit new jobs
    :param username: the username of the submitting user
    :param total_cpu_limit: the total CPU limit for the user (all jobs, not only scheduler ones)
    :param total_job_number_limit: the total job NUMBER limit for the user (all jobs, not only scheduler ones)
    :param scheduler_job_limit: the total job NUMBER limit for the user (just for the scheduler)
    :param job_timeout: the number of maximum hours for a scheduler job to run. set to None if you want no timeout
    :param scheduler_job_prefix: the prefix of job names - will be suffixed with "_JobNumber"
    :param cpu_per_job: how many cpus to allocate per job
    :return: the number of available slots for the scheduler to submit jobs
    """
    # get data
    results_df, scheduler_jobs = get_user_job_stats(username, scheduler_job_prefix)

    # kill all jobs who hit the timeout

    timed_out_jobs = scheduler_jobs[scheduler_jobs['elapsed_time'] > job_timeout]
    if len(timed_out_jobs.index) > 0:
        for index, row in timed_out_jobs.iterrows():
            job_id = row['job_number'].split('.')[0]
            subprocess.run(['qdel', f'{job_id}'])
        results_df, scheduler_jobs = get_user_job_stats(username, scheduler_job_prefix)

    # check general criteria
    if results_df['cpus'].sum() >= total_cpu_limit:
        return 0
    if len(results_df.index) >= total_job_number_limit:
        return 0

    # check specific criteria
    # no jobs currently scheduled
    if len(scheduler_jobs.index) == 0:
        return min(total_cpu_limit // cpu_per_job, scheduler_job_limit)

    if len(scheduler_jobs.index) >= scheduler_job_limit:
        return 0
    if scheduler_jobs['cpus'].sum() >= total_cpu_limit:
        return 0

    # find how many spots are free
    freeJobsByCore = (total_cpu_limit - scheduler_jobs['cpus'].sum()) // cpu_per_job
    freeJobsByNumber = total_job_number_limit - len(scheduler_jobs.index)
    how_many_jobs = min(freeJobsByCore, freeJobsByNumber)

    return how_many_jobs


def schedule_job(sh_text):
    """
    schedules jobs (sh strings) from the queue
    :param sh_text: the current queue message (sh text)
    :return: nothing
    """
    file_path = pathlib.Path().resolve() / 'temp_sh.sh'
    with open(file_path, 'w+') as fp:
        fp.write(sh_text)
    terminal_cmd = f'/opt/pbs/bin/qsub {str(file_path)}'
    subprocess.call(terminal_cmd, shell=True)
    os.remove(file_path)
