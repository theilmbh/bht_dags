"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import os, shutil, glob, logging
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


SLACK_TOKEN = 'xoxp-8710210593-8710210785-17586684384-e5abadd63e'

ANACONDA_PATH = '/usr/local/anaconda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games'
PHY_PATH = "/usr/local/anaconda/envs/phy/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games"

make_klustadir_cmd = "mkdir -p {{ params.klustadir }}"

make_kwd_command = "make_kwd {{ params.rig }} {{ params.probe }} {{ params.matfiledir }} {{ params.klustadir }} -s 31250 -a none"

def on_kwd_failure(context):
    # clear out the klusta dir
    pass

# sort spikes
sort_spikes_command = "cd {{ params.klustadir }} ; phy spikesort params.prm"

clear_phy_cmd = "rm -rf {{ params.klustadir }}*.phy"

# merge events
merge_events_cmd = "merge_stim_kwik {{ params.matfiledir }}/ {{ params.klustadir }}"

# make kwik bakup dir
make_kwik_bak_dir_cmd = "mkdir -p {{ params.kwikbakdir }}"

# make mansort dir
make_mansort_dir_cmd = "ssh brad@niao.ucsd.edu mkdir -p {{ params.mansortdir }}"

# 
mv_kwik_bak_cmd = "mv {{ params.klustadir }}*.kwik.bak {{ params.kwikbakdir }}"

# rsync
rsync_command = "rsync -azP -r {{ params.klustadir }} {{ params.mansorthost }}:{{ params.mansortdir }}"


with open('/mnt/lintu/home/Gentnerlab/airflow/dags/bht_birds.tsv','r') as f:

    for line in f:
        args = line.strip().split()
        if len(args) < 2:
            continue
        BIRD = args[0]
        BLOCK = args[1]
        OMIT = ''
        
        KLUSTA_DIR = '/mnt/lintu/home/Gentnerlab/sharedata/Ice/%s/klusta/%s/' % (BIRD, BLOCK)
        MATFILE_DIR = '/mnt/lintu/home/btheilma/experiments/%s/matfiles/%s/' % (BIRD, BLOCK)
        KWIKBAK_DIR = '/mnt/cube/btheilma/kwik_bak/%s/' % BIRD
        MANSORT_HOST = 'brad@niao.ucsd.edu'
        MANSORT_DIR = '/home/brad/experiments/%s/klust/%s' % (BIRD, BLOCK)

        PROBE = "A1x16-5mm-50"
        RIG = "burung16"

        dag_id = USER + BLOCK
        dag = DAG(dag_id, 
                  default_args=default_args,
                  schedule_interval='@once',
        )

        make_klusta_dir_task = BashOperator(
            task_id='make_klusta_dir',
            bash_command=make_klustadir_cmd,
            params={'klustadir': KLUSTA_DIR},
            on_success_callback = lambda c: set_perms(c['params']['target_dir'],default_args['owner']), 
            dag=dag)

        make_kwd_task = BashOperator(
            task_id='make_kwd',
            pool='make_kwd',
            bash_command=make_kwd_command,
            env={'PATH': ANACONDA_PATH},
            params={'klustadir': KLUSTA_DIR,
                    'matfiledir': MATFILE_DIR,
                    'probe': PROBE,
                    'rig': RIG},
            on_failure_callback = lambda c: clean_dir(c['params']['klustadir']),
            on_success_callback = lambda c: set_perms(c['params']['klustadir'],default_args['owner']),
            dag=dag)

        phy_task = BashOperator(
            task_id='phy_spikesort',
            pool='phy',
            env={'PATH': PHY_PATH},
            bash_command=sort_spikes_command,
            params={'klustadir': KLUSTA_DIR,
                    'matfiledir': MATFILE_DIR},
            on_failure_callback = lambda c: [clean_dir(c['params']['klustadir'],filt) for filt in ('*.kwik','*.kwx')],
            on_success_callback = lambda c: set_perms(c['params']['klustadir'],default_args['owner']),
            dag=dag)

        #merge_events_task = BashOperator(
        #    task_id='merge_events',
        #    bash_command=merge_events_cmd,
        #    env={'PATH': ANACONDA_PATH},
        #    params={'block': BLOCK},
        #    dag=dag)

        clear_phy_task = BashOperator(
            task_id='clear_phy',
            bash_command=clear_phy_cmd,
            params={'klustadir': KLUSTA_DIR,
                    'matfiledir': MATFILE_DIR},
            dag=dag)

        make_kwik_bak_dir_task = BashOperator(
        	task_id='make_kwik_bak_dir',
        	bash_command=make_kwik_bak_dir_cmd,
        	params={'kwikbakdir': KWIKBAK_DIR},
        	dag=dag)

        mv_kwik_bak_task = BashOperator(
            task_id='move_kwik_bak',
            bash_command=mv_kwik_bak_cmd,
            params={'klustadir': KLUSTA_DIR,
            		'kwikbakdir': KWIKBAK_DIR},
            dag=dag)

        make_mansort_dir_task = BashOperator(
            task_id='make_mansort_dir',
            bash_command=as_user(make_mansort_dir_cmd, USER),
            params={'mansortdir': MANSORT_DIR},
            dag=dag)

        rsync_task = BashOperator(
            task_id='rsync',
            bash_command=as_user(rsync_command, USER),
            params={'klustadir': KLUSTA_DIR,
                    'mansortdir': MANSORT_DIR,
                    'mansorthost': MANSORT_HOST},
            dag=dag)

        email_me = EmailOperator(
            task_id='email_me',
            to=default_args['email'],
            subject='%s is complete' % dag_id,
            html_content='You may now manually sort on NIAO',
            dag=dag)

        slack_it = SlackAPIPostOperator(
            task_id='slack_it',
            token=SLACK_TOKEN,
            text='%s is complete' % dag_id,
            channel='#ephys',
            dag=dag)

        make_kwd_task.set_upstream(make_klusta_dir_task)
        phy_task.set_upstream(make_kwd_task)
        #merge_events_task.set_upstream(phy_task)
        clear_phy_task.set_upstream(phy_task)
        make_kwik_bak_dir_task.set_upstream(phy_task)
        mv_kwik_bak_task.set_upstream(make_kwik_bak_dir_task)
        make_mansort_dir_task.set_upstream(phy_task)
        rsync_task.set_upstream(clear_phy_task)
        rsync_task.set_upstream(mv_kwik_bak_task)
        rsync_task.set_upstream(make_mansort_dir_task)
        email_me.set_upstream(rsync_task)
        slack_it.set_upstream(rsync_task)
     
        globals()[dag_id] = dag
