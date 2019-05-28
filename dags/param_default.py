from datetime import datetime, timedelta
param_default = {
    "NAME":"minnie_367_0",

    "SCRATCH_PREFIX":"gs://ranl-scratch/",

    "AFF_PATH":"gs://microns-seunglab/minnie_v0/minnie10/affinitymap/test",
    "AFF_MIP":"1",

    "WS_PREFIX":"gs://microns-seunglab/minnie_v0/minnie10/ws_",

    "SEG_PREFIX":"gs://microns-seunglab/minnie_v0/minnie10/seg_",

    "WS_HIGH_THRESHOLD":"0.99",
    "WS_LOW_THRESHOLD":"0.01",
    "WS_SIZE_THRESHOLD":"200",

    "AGG_THRESHOLD":"0.25",
    "WS_IMAGE":"ranlu/watershed:ranl_minnie_test",
    "AGG_IMAGE":"ranlu/agglomeration:ranl_minnie_test",
    "BBOX": [126280, 64280, 20826, 129020, 129020, 20993],
    "RESOLUTION": [8,8,40],
    "CHUNK_SIZE": [512, 512, 128],
    "SKIP_WS": False,
    "SKIP_AGG": False,
    "SKIP_DM": False
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 2, 28),
    'cactchup_by_default': False,
    'retries': 100,
    'retry_delay': timedelta(seconds=10),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(seconds=600)
    }


SLACK_CONN_ID = 'Slack'
AWS_CONN_ID = 'AWS'
CLUSTER_1_CONN_ID = "InstanceGroup1"
CLUSTER_2_CONN_ID = "InstanceGroup2"


cv_path = "/root/.cloudvolume/secrets/"
cmd_proto = '/bin/bash -c "mkdir $AIRFLOW_TMP_DIR/work && cd $AIRFLOW_TMP_DIR/work && {} && rm -rf $AIRFLOW_TMP_DIR/work || {{ rm -rf $AIRFLOW_TMP_DIR/work; exit 111; }}"'

cv_chunk_size=[128,128,16]
batch_mip = 3
high_mip = 5
