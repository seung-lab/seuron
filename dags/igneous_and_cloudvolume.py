from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from cloudvolume import CloudVolume
from taskqueue import TaskQueue
from cloudvolume.lib import Vec
import igneous.task_creation as tc
import os
from time import sleep, strftime

from param_default import cv_chunk_size, AWS_CONN_ID
from slack_message import slack_message, slack_userinfo


def create_info(stage, param):
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)


    for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    bbox = param["BBOX"]
    metadata_seg = CloudVolume.create_new_info(
        num_channels    = 1,
        layer_type      = 'segmentation',
        data_type       = 'uint64',
        encoding        = 'raw',
        resolution      = param["RESOLUTION"], # Pick scaling for your data!
        voxel_offset    = bbox[0:3],
        chunk_size      = cv_chunk_size, # This must divide evenly into image length or you won't cover the #
        volume_size     = [bbox[i+3] - bbox[i] for i in range(3)]
        )
    cv_path = param["WS_PATH"] if stage == "ws" else param["SEG_PATH"]
    vol = CloudVolume(cv_path, mip=0, info=metadata_seg)
    author = slack_userinfo()
    if author is None:
        author = "seuronbot"
    vol.commit_info()
    vol.provenance.processing.append({
        'method': param,
        'by': author,
        'date': strftime('%Y-%m-%d %H:%M %Z')
    })
    vol.commit_provenance()

    for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
        os.remove(os.path.join(cv_secrets_path, k))


def check_queue(tq):
    totalTries = 5
    nTries = totalTries
    while True:
        sleep(20)
        nTasks = tq.enqueued
        print("Tasks left: {}".format(nTasks))
        if nTasks == 0:
            nTries -= 1
        else:
            nTries = totalTries
        if nTries == 0:
            return


def downsample_and_mesh(param):
    if param["SKIP_DM"]:
        slack_message(":exclamation: Skip downsample and mesh as instructed")
        return

    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)


    for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)


    seg_cloudpath = param["SEG_PATH"]

    try:
        os.environ['AWS_ACCESS_KEY_ID'] = BaseHook.get_connection(AWS_CONN_ID).login
        os.environ['AWS_SECRET_ACCESS_KEY'] = BaseHook.get_connection(AWS_CONN_ID).password
        url = BaseHook.get_connection(AWS_CONN_ID).host

        mesh_mip = 3 - int(param["AFF_MIP"])
        #cube_dim = 512//(2**(mesh_mip+1))

        with TaskQueue(url, queue_server='sqs') as tq:
            tc.create_downsampling_tasks(tq, seg_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
            check_queue(tq)
            slack_message(":arrow_forward: Downsampled")
            tc.create_meshing_tasks(tq, seg_cloudpath, mip=mesh_mip, shape=Vec(256, 256, 256))
            check_queue(tq)
            slack_message(":arrow_forward: Meshed")
            tc.create_mesh_manifest_tasks(tq, seg_cloudpath, magnitude=2)
            check_queue(tq)
            slack_message(":arrow_forward: Manifest genrated")
            #tc.create_downsampling_tasks(tq, seg_cloudpath, mip=5, fill_missing=True, preserve_chunk_size=True)
            #check_queue(tq)
    except:
        slack_message(":exclamation: Incorrect AWS SQS settings, cannot downsample or mesh")
        return
    finally:
        for k in ['neuroglancer-google-secret.json', 'google-secret.json']:
            os.remove(os.path.join(cv_secrets_path, k))

