from airflow.models import Variable
from cloudvolume import CloudVolume, Storage
from cloudvolume.lib import Vec
import os
import requests
from time import sleep, strftime

from slack_message import slack_message, slack_userinfo
from param_default import CLUSTER_1_CONN_ID
from google_api_helper import reduce_instance_group_size, increase_instance_group_size


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
    return [int(x) for x in list(vol.resolution)]

def check_cloud_path_empty(path):
    s = Storage(path)
    try:
        obj = next(s.list_files(), None)
    except:
        slack_message(""":exclamation:*Error*: Check cloud path failed: {}, does the bucket exist?""".format(path))
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
    resolution = dataset_resolution(param["AFF_PATH"], int(param["AFF_MIP"]))
    cv_chunk_size = param.get("CV_CHUNK_SIZE", [128,128,16])
    metadata_seg = CloudVolume.create_new_info(
        num_channels    = 1,
        layer_type      = 'segmentation',
        data_type       = 'uint64',
        encoding        = 'raw',
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
    vol.commit_info()
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


def get_eval_job(v, param):
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        for c in v:
            if c.mip_level() != 0:
                continue
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('agg/evaluation/evaluation_{}.data'.format(tag))

    return content


def get_info_job(v, param):
#    try:
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        for c in v:
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('agg/info/info_{}.data'.format(tag))

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
    if param.get("SKIP_DOWNSAMPLE", False):
        slack_message(":exclamation: Skip downsample (and meshing) as instructed")
        return

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

    mesh_mip = 3 - int(param["AFF_MIP"])
    simplification = True
    max_simplification_error = 40
    if param.get("MESH_QUALITY", "NORMAL") == "PERFECT":
        simplification = False
        max_simplification_error = 0
        mesh_mip = 0
    #cube_dim = 512//(2**(mesh_mip+1))

    with Connection(broker, connect_timeout=60) as conn:
        queue = conn.SimpleQueue("igneous")
        if not param.get("SKIP_WS", False):
            tasks = tc.create_downsampling_tasks(ws_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
            for t in tasks:
                submit_task(queue, t.payload())

        tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
        increase_instance_group_size("igneous", min(50, len(tasks)//32))
        for t in tasks:
            submit_task(queue, t.payload())

        if not param.get("SKIP_AGG", False):
            tasks = tc.create_downsampling_tasks(seg_cloudpath+"/size_map", mip=0, fill_missing=True, preserve_chunk_size=True)
            for t in tasks:
                submit_task(queue, t.payload())

        check_queue("igneous")
        slack_message(":arrow_forward: Downsampled")

        if param.get("SKIP_MESHING", False):
            slack_message(":exclamation: Skip meshing as instructed")
            return

        #if param.get("SKIP_AGG", False):
        #    slack_message(":exclamation: No segmentation generated, skip meshing")
        #    return

        vol = CloudVolume(seg_cloudpath)
        if mesh_mip not in vol.available_mips:
            mesh_mip = max(vol.available_mips)
        slack_message("Mesh at resolution: {}".format(vol.scales[mesh_mip]['key']))

        tasks = tc.create_meshing_tasks(seg_cloudpath, mip=mesh_mip, simplification=simplification, max_simplification_error=max_simplification_error, shape=Vec(256, 256, 256))
        for t in tasks:
            submit_task(queue, t.payload())
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
        reduce_instance_group_size("igneous", 10)
        for p in sorted(prefix_list, key=len):
            if any(p.startswith(s) for s in task_list):
                print("Already considered, skip {}".format(p))
                continue
            task_list.append(p)
            t = MeshManifestTask(layer_path=seg_cloudpath, prefix=str(p))
            submit_task(queue, t.payload())
        print("total number of tasks: {}".format(len(task_list)))

        check_queue("igneous")
        slack_message(":arrow_forward: Manifest genrated")
        queue.close()
        #tc.create_downsampling_tasks(tq, seg_cloudpath, mip=5, fill_missing=True, preserve_chunk_size=True)
        #check_queue(tq)


