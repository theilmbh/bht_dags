"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators import BashOperator, EmailOperator, SlackAPIPostOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'btheilma',
    'start_date': datetime(2015,12,01),
    'email': ['btheilma@ucsd.edu'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

USER = 'btheilma'
def as_user(cmd,username):
    return "runuser -l %s -m -c '%s'" % (username,cmd)

SLACK_TOKEN = 'xoxp-8710210593-8710210785-17586684384-e5abadd63e'

ANACONDA_PATH = '/usr/local/anaconda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games'
PHY_PATH = "/usr/local/anaconda/envs/phy/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games"

make_klustadir_cmd = "mkdir -p /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }}"

make_kwd_command = "make_kwd burung16 A1x16-5mm-50 /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/matfiles/{{ params.block }}/ /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }} -s 31250 -a none"

def on_kwd_failure(context):
    # clear out the klusta dir
    pass

# sort spikes
sort_spikes_command = "cd /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }} ; phy spikesort params.prm"

clear_phy_cmd = "rm -rf /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }}/*.phy"

# merge events
merge_events_cmd = "merge_stim_kwik /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/matfiles/{{ params.block }}/ /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }}"

# make kwik bakup dir
make_kwik_bak_dir_cmd = "mkdir -p /mnt/cube/btheilma/kwik_bak/{{ params.birdid }}"

# 
mv_kwik_bak_cmd = "mv /mnt/lintu/home/btheilma/experiments/{{ params.birdid }}/klusta/{{ params.block }}/*.kwik.bak /mnt/cube/btheilma/kwik_bak/{{ params.birdid }}"

# rsync
rsync_command = "nice +5 rsync -azP --relative /mnt/lintu/home/btheilma/experiments/{{ params.bird }}/klusta/{{ params.block }} btheilma@niao.ucsd.edu:/home/btheilma/experiments/"


with open('/mnt/lintu/home/Gentnerlab/airflow/dags/bht_birds.tsv','r') as f:

    for line in f:
        args = line.strip().split()
        if len(args) < 2:
            continue
        BIRD = args[0]
        BLOCK = args[1]
        OMIT = ''
        
        dag_id = USER + BLOCK
        dag = DAG(dag_id, 
                  default_args=default_args,
                  schedule_interval='@once',
        )

        make_klusta_dir_task = BashOperator(
            task_id='make_klusta_dir',
            bash_command=as_user(make_klustadir_cmd, USER),
            params={'block': BLOCK,
            		'birdid': BIRD},
            dag=dag)

        make_kwd_task = BashOperator(
            task_id='make_kwd',
            pool='make_kwd',
            bash_command=as_user(make_kwd_command, USER),
            env={'PATH': ANACONDA_PATH},
            params={'block': BLOCK,
                    'omit': OMIT,
                    'birdid': BIRD},
            dag=dag)

        phy_task = BashOperator(
            task_id='phy_spikesort',
            pool='phy',
            env={'PATH': PHY_PATH},
            bash_command=as_user(sort_spikes_command, USER),
            params={'block': BLOCK,
            		'birdid': BIRD},
            dag=dag)

        #merge_events_task = BashOperator(
        #    task_id='merge_events',
        #    bash_command=merge_events_cmd,
        #    env={'PATH': ANACONDA_PATH},
        #    params={'block': BLOCK},
        #    dag=dag)

        clear_phy_task = BashOperator(
            task_id='clear_phy',
            bash_command=as_user(clear_phy_cmd, USER),
            params={'block': BLOCK,
            		'birdid': BIRD},
            dag=dag)

        make_kwik_bak_dir_task = BashOperator(
        	task_id='make_kwik_bak_dir',
        	bash_command=as_user(make_kwik_bak_dir_cmd, USER),
        	params={'birdid': BIRD},
        	dag=dag)

        mv_kwik_bak_task = BashOperator(
            task_id='move_kwik_bak',
            bash_command=as_user(mv_kwik_bak_cmd, USER),
            params={'block': BLOCK,
            		'birdid': BIRD},
            dag=dag)

        rsync_task = BashOperator(
            task_id='rsync',
            bash_command=as_user(rsync_command, USER),
            params={'block': BLOCK},
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
        #rsync_task.set_upstream(merge_events_task)
        rsync_task.set_upstream(clear_phy_task)
        rsync_task.set_upstream(mv_kwik_bak_task)
        email_me.set_upstream(rsync_task)
        slack_it.set_upstream(rsync_task)
     
        globals()[dag_id] = dag