from datetime import datetime, timedelta


def check_worker_image_labels(variable):
    import docker
    from airflow.models import Variable
    if variable == "param":
        image_name = "WORKER_IMAGE"
        default_path = default_seg_workspace
    elif variable == "inference_param":
        image_name = "CHUNKFLOW_IMAGE"
        default_path = default_chunkflow_workspace

    param = Variable.get(variable, deserialize_json=True)
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    image = client.images.pull(param[image_name])
    param["WORKSPACE_PATH"] = image.labels.get("workspace_path", default_path)
    param["MOUNT_PATH"] = image.labels.get("mount_path", default_mount_path)
    Variable.set(variable, param, serialize_json=True)


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
    "WORKER_IMAGE":"ranlu/segmentation:ranl_testing",
    "BBOX": [127280, 127280, 20826, 129020, 129020, 20993],
    "CHUNK_SIZE": [512, 512, 128],
}

inference_param_default = {
    "NAME":"test_affinity",

    "IMAGE_PATH":"gs://zetta_lee_fly_vnc_001_cutouts/010/image",

    "CHUNKFLOW_IMAGE":"ranlu/chunkflow:test",

    "BBOX": [7168, 20480, 1500, 8608, 21920, 1764],
}

default_synaptor_param = """
[Workflow]
maxclustersize = 1
"""

default_args = {
    'owner': 'seuronbot',
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
CLUSTER_1_CONN_ID = "atomic"
CLUSTER_2_CONN_ID = "composite"


default_mount_path = "/root/.cloudvolume/secrets/"
default_chunkflow_workspace = "/root/workspace/chunkflow"
default_seg_workspace = "/root/seg"

cmd_proto = '/bin/bash -c "mkdir $AIRFLOW_TMP_DIR/work && cd $AIRFLOW_TMP_DIR/work && {} && rm -rf $AIRFLOW_TMP_DIR/work || {{ rm -rf $AIRFLOW_TMP_DIR/work; exit 111; }}"'

batch_mip = 3
high_mip = 5
