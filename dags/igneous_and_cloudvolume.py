from airflow.models import Variable
from cloudvolume import CloudVolume, Storage
from cloudvolume.lib import Vec
import os
import requests
from time import sleep, strftime
from math import log2

from slack_message import slack_message, slack_userinfo
from google_api_helper import ramp_up_cluster, ramp_down_cluster, reset_cluster


import tenacity

retry = tenacity.retry(
  reraise=True,
  stop=tenacity.stop_after_attempt(10),
  wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
)

@retry
def submit_task(queue, payload):
    queue.put(payload)

def dataset_resolution(path, mip=0):
    vol = CloudVolume(path, mip=mip)
    return vol.resolution.tolist()

def check_cloud_path_empty(path):
    import traceback
    try:
        s = Storage(path)
        obj = next(s.list_files(), None)
    except:
        slack_message(""":exclamation:*Error*: Check cloud path failed:
```{}``` """.format(traceback.format_exc()))
        raise

    if obj is not None:
        slack_message(""":exclamation:*Error*: `{}` is not empty""".format(path))
        raise RuntimeError('Path already exist')

def create_info(stage, param):
    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)

    bbox = param["BBOX"]
    resolution = param["AFF_RESOLUTION"]
    cv_chunk_size = param.get("CV_CHUNK_SIZE", [256,256,32])
    metadata_seg = CloudVolume.create_new_info(
        num_channels    = 1,
        layer_type      = 'segmentation',
        data_type       = 'uint64',
        encoding        = 'compressed_segmentation',
        resolution      = resolution, # Pick scaling for your data!
        voxel_offset    = bbox[0:3],
        chunk_size      = cv_chunk_size, # This must divide evenly into image length or you won't cover the #
        volume_size     = [bbox[i+3] - bbox[i] for i in range(3)]
        )
    cv_path = param["WS_PATH"] if stage == "ws" else param["SEG_PATH"]
    vol = CloudVolume(cv_path, mip=0, info=metadata_seg)
    author = slack_userinfo()
    if author is None:
        author = "seuronbot"

    try:
        vol.commit_info()
    except:
        slack_message(""":exclamation:*Error*: Cannot commit cloudvolume info to `{}`, check if the bot have write permission.""".format(cv_path))
        raise

    vol.provenance.processing.append({
        'method': param,
        'by': author,
        'date': strftime('%Y-%m-%d %H:%M %Z')
    })
    vol.commit_provenance()

    if stage == "agg":
        cv_path = param["SEG_PATH"]+"/size_map"
        metadata_size = CloudVolume.create_new_info(
            num_channels    = 1,
            layer_type      = 'image',
            data_type       = 'uint8',
            encoding        = 'raw',
            resolution      = resolution, # Pick scaling for your data!
            voxel_offset    = bbox[0:3],
            chunk_size      = cv_chunk_size, # This must divide evenly into image length or you won't cover the #
            volume_size     = [bbox[i+3] - bbox[i] for i in range(3)]
            )
        vol = CloudVolume(cv_path, mip=0, info=metadata_size)
        vol.commit_info()


    for k in mount_secrets:
        os.remove(os.path.join(cv_secrets_path, k))


def upload_json(path, filename, content):
    with Storage(path) as storage:
        storage.put_json(filename, content)


def get_atomic_files_job(v, param, prefix):
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        for c in v:
            if c.mip_level() != 0:
                continue
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('{}_{}.data'.format(prefix, tag))

    return content


def get_files_job(v, param, prefix):
#    try:
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        for c in v:
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('{}_{}.data'.format(prefix, tag))

    return content

#    except:
#        print("Cannot read all the info files")
#        return None


def check_queue(queue):
    totalTries = 2
    nTries = totalTries
    while True:
        sleep(5)
        ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
        if not ret.ok:
            raise RuntimeError("Cannot connect to rabbitmq management interface")
        queue_status = ret.json()
        nTasks = queue_status["messages"]
        print("Tasks left: {}".format(nTasks))
        if nTasks == 0:
            nTries -= 1
        else:
            nTries = totalTries
        if nTries == 0:
            return


def downsample_and_mesh(param):
    import igneous.task_creation as tc
    from igneous.tasks import MeshManifestTask
    from airflow import configuration
    from kombu import Connection
    from chunk_iterator import ChunkIterator
    from os.path import commonprefix

    #Reuse the broker of celery
    #if param.get("SKIP_DOWNSAMPLE", False):
    #    slack_message(":exclamation: Skip downsample (and meshing) as instructed")
    #    return

    broker = configuration.get('celery', 'BROKER_URL')

    cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
    if not os.path.exists(cv_secrets_path):
        os.makedirs(cv_secrets_path)

    mount_secrets = param.get("MOUNT_SECRETES", [])

    for k in mount_secrets:
        v = Variable.get(k)
        with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
            value_file.write(v)


    seg_cloudpath = param["SEG_PATH"]
    ws_cloudpath = param["WS_PATH"]

    #cube_dim = 512//(2**(mesh_mip+1))
    target_size = 0

    with Connection(broker, connect_timeout=60) as conn:
        queue = conn.SimpleQueue("igneous")

        if not param.get("SKIP_DOWNSAMPLE", False):
            tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=0, fill_missing=True, mask=param.get("SIZE_THRESHOLDED_MESH", False), num_mips=2, preserve_chunk_size=True)
            target_size = (1+len(tasks)//32)
            ramp_up_cluster("igneous", 20, min(50, target_size))
            for t in tasks:
                submit_task(queue, t.payload())

            check_queue("igneous")
            slack_message(":arrow_forward: Downsampled")
        else:
            target_size = 10
            ramp_up_cluster("igneous", 20, min(50, target_size))

        if param.get("SKIP_MESHING", False):
            slack_message(":exclamation: Skip meshing as instructed")
            return

        #if param.get("SKIP_AGG", False):
        #    slack_message(":exclamation: No segmentation generated, skip meshing")
        #    return

        vol = CloudVolume(seg_cloudpath)
        mesh_mip = int(log2(vol.resolution[2]/vol.resolution[0]))

        if mesh_mip not in vol.available_mips:
            mesh_mip = max(vol.available_mips)

        simplification = True
        max_simplification_error = 40
        if param.get("MESH_QUALITY", "NORMAL") == "PERFECT":
            simplification = False
            max_simplification_error = 0
            mesh_mip = 0

        slack_message("Mesh at resolution: {}".format(vol.scales[mesh_mip]['key']))
        if not param.get("SKIP_DOWNSAMPLE", False):
            if (target_size > 20):
                reset_cluster("igneous", 20)

        tasks = tc.create_meshing_tasks(seg_cloudpath, mip=mesh_mip, simplification=simplification, max_simplification_error=max_simplification_error, shape=Vec(256, 256, 256))
        for t in tasks:
            submit_task(queue, t.payload())

        if param.get("SKIP_DOWNSAMPLE", False):
            target_size = (1+len(tasks)//32)
            ramp_up_cluster("igneous", 20, min(50, target_size))

        check_queue("igneous")
        slack_message(":arrow_forward: Meshed")

        #FIXME: should reuse the function in segmentation scripts
        layer = 1
        bits_per_dim = 10
        n_bits_for_layer_id = 8

        layer_offset = 64 - n_bits_for_layer_id
        x_offset = layer_offset - bits_per_dim
        y_offset = x_offset - bits_per_dim
        z_offset = y_offset - bits_per_dim

        chunk_voxels = 1 << (64-n_bits_for_layer_id-bits_per_dim*3)

        v = ChunkIterator(param["BBOX"], param["CHUNK_SIZE"])

        prefix_list = []
        for c in v:
            if c.mip_level() == 0:
                x, y, z = c.coordinate()
                min_id = layer << layer_offset | x << x_offset | y << y_offset | z << z_offset
                max_id = min_id + chunk_voxels
                if len(str(min_id)) != len(str(max_id)):
                    raise NotImplementedError("No common prefix, need to split the range")
                prefix = commonprefix([str(min_id), str(max_id)])
                if len(prefix) == 0:
                    raise NotImplementedError("No common prefix, need to split the range")
                digits = len(str(min_id)) - len(prefix) - 1
                mid = int(prefix+str(max_id)[len(prefix)]+"0"*digits)
                #print(int(mid),int(mid)-1)
                prefix1 = commonprefix([str(min_id), str(mid-1)])
                prefix2 = commonprefix([str(max_id), str(mid)])
                if len(prefix1) <= len(prefix):
                    #print("min_id {}, max_id {}, prefix {}".format(min_id, max_id, prefix))
                    prefix_list.append(prefix)
                else:
                    #print("min_id {}, max_id {}, mid {}, prefix {}, {}".format(min_id, max_id, mid, prefix1, prefix2))
                    prefix_list.append(prefix1)
                    prefix_list.append(prefix2)

        task_list = []
        if (target_size > 10):
            reset_cluster("igneous", 10)
        for p in sorted(prefix_list, key=len):
            if any(p.startswith(s) for s in task_list):
                print("Already considered, skip {}".format(p))
                continue
            task_list.append(p)
            t = MeshManifestTask(layer_path=seg_cloudpath, prefix=str(p))
            submit_task(queue, t.payload())
        print("total number of tasks: {}".format(len(task_list)))

        if not param.get("SKIP_DOWNSAMPLE", False):
            if not param.get("SKIP_WS", False):
                tasks = tc.create_downsampling_tasks(ws_cloudpath, mip=0, fill_missing=True, num_mips=2, preserve_chunk_size=True)
                for t in tasks:
                    submit_task(queue, t.payload())
            if "SEM_PATH" in param and param.get("DOWNSAMPLE_SEM", False):
                tasks = tc.create_downsampling_tasks(param["SEM_PATH"], mip=param["AFF_MIP"], fill_missing=True, num_mips=2, preserve_chunk_size=True)
                for t in tasks:
                    submit_task(queue, t.payload())

            tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=0, fill_missing=True, num_mips=2, preserve_chunk_size=True)
            target_size = (1+len(tasks)//32)
            ramp_up_cluster("igneous", 20, min(50, target_size))
            for t in tasks:
                submit_task(queue, t.payload())

            #if not param.get("SKIP_AGG", False):
            tasks = tc.create_downsampling_tasks(seg_cloudpath+"/size_map", mip=0, fill_missing=True, num_mips=2, preserve_chunk_size=True)
            for t in tasks:
                submit_task(queue, t.payload())

        check_queue("igneous")
        slack_message(":arrow_forward: Manifest genrated")
        queue.close()
        #tc.create_downsampling_tasks(tq, seg_cloudpath, mip=5, fill_missing=True, preserve_chunk_size=True)
        #check_queue(tq)


