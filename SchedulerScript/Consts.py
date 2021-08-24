
QstatDataColumns = ['job_number', 'username', 'queue', 'job_name', 'session_id', 'nodes', 'cpus', 'req_mem',
                    'req_time', 'job_status', 'elapsed_time']

ShBaseText ='''
    #!/bin/bash

    #PBS -S /bin/bash
    #PBS -r y
    #PBS -l ncpus={ncpus}
    #PBS -q {queue}
    #PBS -v PBS_O_SHELL=bash,PBS_ENVIRONMENT=PBS_BATCH
    #PBS -N {job_base_name}{job_unique_id}
    #PBS -e {error_path}
    #PBS -o {out_path}

    echo 'job_name: {job_base_name}{job_unique_id}\n'
    echo 'queue: {queue}\n'
    echo 'Job ID\n'
    echo $PBS_JOBID\n
    echo 'cpus: {ncpus}\n'
    echo 'hostname\n'
    hostname
    echo 'script params: {script_param_text} \n'
    echo '\n'

    {conda} 

    {script_exe_command}  {script_path}  {script_param_text}
    '''