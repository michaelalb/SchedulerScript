import math
import queue
from pathlib import Path

import Consts
import numpy as np
import subprocess
import re
import pandas as pd
import os

def create_base_sh(script_path, queue_name, script_exec_command, error_path, job_base_name, ncpus, out_path,
                   conda_env_name, conda_env_path):
    if conda_env_name is not None and conda_env_path is not None:
        conda_text = f'source {conda_env_path} \n conda activate {conda_env_name}'
    else:
        conda_text = ''
    return Consts.ShBaseText.format(ncpus=ncpus, queue_name=queue_name, job_base_name=job_base_name,
                                    error_path=str(error_path), out_path=str(out_path), conda=conda_text,
                                    script_exe_command=script_exec_command,script_path=script_path)


def split_data_sets_into_chunks(data_params, chunk_size=2):
    """
    this function splits all data params
    :param data_params: list of data parmas
    :param chunk_size:
    :return:
    """
    assert type(data_params).isin([list, np.array, np.ndarray])
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
        paramText = baseParamText + f' --{data_param_name} {str(dataParamChunk)}'
        currentJobSh = base_sh.format(job_unique_id=jobNumber, script_param_text=paramText)
        taskQueue.put(currentJobSh, block=True)

    return taskQueue


def get_how_many_free_slots(username, total_cpu_limit, total_job_number_limit, scheduler_job_limit, job_timeout,
                            scheduler_job_prefix, cpu_per_job):
    # get data
    result = subprocess.run(['qstat', f'-u {username}'], stdout=subprocess.PIPE)
    result_lines = (str(result.stdout).split('\\n'))[5:-1]  # irrelevant text from qstat
    results_params = [re.sub('\s+', ' ', x).split(' ') for x in result_lines]  # remove spaces and turn to data
    results_df = pd.DataFrame(results_params, columns=Consts.QstatDataColumns)
    results_df = results_df[results_df['job_name'].str.find('SCLoop')!=-1] # ignore the scheduling job itself

    # check general criteria
    if results_df['cpus'].sum > total_cpu_limit:
        return 0
    if len(results_df.index) > total_job_number_limit:
        return 0

    # check specific criteria
    scheduler_jobs = results_df[results_df['job_name'].str.starwith(scheduler_job_prefix)]
    if len(scheduler_jobs.index) > scheduler_job_limit:
        return 0
    scheduler_jobs['elapsed_time'] = scheduler_jobs['elapsed_time'].astype(str)
    scheduler_jobs['elapsed_time'] = scheduler_jobs['elapsed_time'].split(':')[0]  # just care about the hours

    # find how many spots are free
    freeJobsByCore = (total_cpu_limit - scheduler_jobs['cpus'].sum) // cpu_per_job
    freeJobsByNumber = total_job_number_limit - len(scheduler_jobs.index)
    how_many_jobs = min(freeJobsByCore, freeJobsByNumber)

    # kill all jobs who hit the timeout
    scheduler_jobs = scheduler_jobs[scheduler_jobs['elapsed_time'] > job_timeout]
    if len(scheduler_jobs.index) > 0:
        for index, row in scheduler_jobs.iterrows():
            job_id = row['job_number'].split('.')[0]
            subprocess.run(['qdel', f'{job_id}'])
            how_many_jobs += 1
    return how_many_jobs


def schedule_job(sh_text):
    file_path = Path(os.curdir) / 'temp_sh.sh'
    with open(file_path, 'w+') as fp:
        fp.write(sh_text)
    terminal_cmd = f'/opt/pbs/bin/qsub {str(file_path)}'
    subprocess.call(terminal_cmd, shell=True)
    os.remove(file_path)

