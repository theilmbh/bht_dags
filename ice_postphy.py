"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import os, shutil, glob, logging, pwd
from airflow import DAG
from airflow.operators import BashOperator, EmailOperator, SlackAPIPostOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'btheilma',
    'start_date': datetime(2015,12,01),
    'email': ['btheilma@ucsd.edu;kperks@ucsd.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

USER = 'btheilma'
def as_user(cmd,username):
    return "sudo -u %s sh -c '%s'" % (username,cmd)


def clean_dir(folder,filt='*'):
    ''' cleans the folder subject to the filter

    equivalent to 
        rm -rf {folder}/{filter}

    '''
    for file_path in glob.glob(os.path.join(folder,filt)):
        logging.warning('removing %s' % file_path)
        if os.path.isfile(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path): 
            shutil.rmtree(file_path)
    return True

def set_perms(path,username):
    logging.info('changing owner of %s to %s' % (path,username))
    rec = pwd.getpwnam(username)
    for root, dirs, files in os.walk(path):  
        for d in dirs:  
            os.chown(os.path.join(root, d), rec.pw_uid, rec.pw_gid)
        for f in files:
            os.chown(os.path.join(root, f), rec.pw_uid, rec.pw_gid)
    return True


ANACONDA_PATH = '/usr/local/anaconda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games'
PHY_PATH = "/usr/local/anaconda/envs/phy/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games"
ECANALYSIS_PATH = "/mnt/lintu/home/btheilma/code/ECAnalysis"

make_postphy_dir_cmd = "mkdir -p {{ params.postphydir }}"

# merge events
merge_events_cmd = "merge_stim_kwik {{ params.matfiledir }}/ {{ params.postphydir }}"

# rsync
rsync_command = "rsync -azP -r {{ params.mansorthost }}:{{ params.mansortdir }} {{ params.postphydir }}"

# kwik2pandas
kwik2pandas_cmd = "{{ params.ecanalysispath }}/kwik2pandasBT.py {{ params.postphydir }} {{ params.postphydir }}"

# make_raster_dir
make_raster_dir_cmd = "mkdir -p {{ params.rasterdir }}"

# make_raster_cmd
make_raster_cmd = "{{ params.ecanalysispath }}/make_raster_py.py {{ params.postphydir }} {{ params.rasterdir }}"

with open('/mnt/lintu/home/Gentnerlab/airflow/dags/ice_birds_postphy.tsv','r') as f:

    for line in f:
        args = line.strip().split()
        if len(args) < 3:
            continue
        BIRD = args[0]
        BLOCK = args[1]
        SORT_ID = args[2]
        
        
        MATFILE_DIR = '/mnt/cube/Ice/%s/matfiles/%s/' % (BIRD, BLOCK)
        KWIKBAK_DIR = '/mnt/cube/btheilma/kwik_bak/%s/klusta/%s' % (BIRD, BLOCK)
        MANSORT_HOST = 'brad@niao.ucsd.edu'
        MANSORT_DIR = '/home/brad/experiments/%s/klusta/%s/' % (BIRD, BLOCK)
        POSTPHY_DIR = '/mnt/lintu/home/Gentnerlab/Ice/%s/postphy_%s/%s/' % (BIRD, SORT_ID, BLOCK)
        RASTER_DIR = '/mnt/lintu/home/Gentnerlab/Ice/%s/postphy_%s/%s/rasters/' % (BIRD, SORT_ID, BLOCK)

        PROBE = "A1x16-5mm-50"
        RIG = "burung16"

        dag_id = "postphy_" + BIRD + "_" + USER + "_" + BLOCK
        dag = DAG(dag_id, 
                  default_args=default_args,
                  schedule_interval='@once',
        )
    ############ Post-phy cleanup and merging
        make_postphy_dir_task = BashOperator(
            task_id='make_postphy_dir',
            bash_command=as_user(make_postphy_dir_cmd, USER),
            params={'postphydir': POSTPHY_DIR},
            on_success_callback = lambda c: set_perms(c['params']['postphydir'],default_args['owner']), 
            dag=dag)

        rsync_task = BashOperator(
            task_id='rsync',
            bash_command=as_user(rsync_command, USER),
            params={'postphydir': POSTPHY_DIR,
                    'mansortdir': MANSORT_DIR,
                    'mansorthost': MANSORT_HOST},
            dag=dag)

        merge_events_task = BashOperator(
            task_id='merge_events',
            bash_command=merge_events_cmd,
            env={'PATH': ANACONDA_PATH},
            params={'matfiledir': MATFILE_DIR,
                    'postphydir': POSTPHY_DIR},
            dag=dag)

        kwik2pandas_task = BashOperator(
            task_id='kwik2pandas',
            bash_command=kwik2pandas_cmd,
            env={'PATH': ANACONDA_PATH},
            params={'postphydir': POSTPHY_DIR,
                    'ecanalysispath': ECANALYSIS_PATH},
            dag=dag)

    ############ Rasters
        make_raster_dir_task = BashOperator(
            task_id='make_raster_dir',
            bash_command=make_raster_dir_cmd,
            params={'rasterdir': RASTER_DIR},
            on_success_callback = lambda c: set_perms(c['params']['rasterdir'],default_args['owner']), 
            dag=dag)

        make_raster_task = BashOperator(
            task_id='make_rasters',
            bash_command=make_raster_cmd,
            env={'PATH': ANACONDA_PATH},
            params={'postphydir': POSTPHY_DIR,
                    'ecanalysispath': ECANALYSIS_PATH,
                    'rasterdir': RASTER_DIR},
            dag=dag)

    ############ Report Completion
        email_me = EmailOperator(
            task_id='email_me',
            to=default_args['email'],
            subject='%s is merged' % dag_id,
            html_content='You may commence analysis.',
            dag=dag)


        rsync_task.set_upstream(make_postphy_dir_task)
        merge_events_task.set_upstream(rsync_task)
        kwik2pandas_task.set_upstream(merge_events_task)
        email_me.set_upstream(kwik2pandas_task)
        make_raster_dir_task.set_upstream(kwik2pandas_task)
        make_raster_task.set_upstream(make_raster_dir_task)
     
        globals()[dag_id] = dag
