from airflow.models import Variable
from cloudvolume import CloudVolume, Storage
from cloudvolume.lib import Vec
import os
import requests
from time import sleep, strftime

from slack_message import slack_message, slack_userinfo


def dataset_resolution(path, mip=0):
    vol = CloudVolume(path, mip=mip)
    return [int(x) for x in list(vol.resolution)]


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
    if param.get("MESH_QUALITY", "LOW") == "HIGH":
        simplification = False
        max_simplification_error = 0
        mesh_mip = 0
    #cube_dim = 512//(2**(mesh_mip+1))

    with Connection(broker) as conn:
        queue = conn.SimpleQueue("igneous")
        if not param.get("SKIP_WS", False):
            tasks = tc.create_downsampling_tasks(ws_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
            for t in tasks:
                queue.put(t.payload())

        if not param.get("SKIP_AGG", False):
            tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=0, fill_missing=True, preserve_chunk_size=True)
            for t in tasks:
                queue.put(t.payload())
            tasks = tc.create_downsampling_tasks(seg_cloudpath+"/size_map", mip=0, fill_missing=True, preserve_chunk_size=True)
            for t in tasks:
                queue.put(t.payload())

        check_queue("igneous")
        slack_message(":arrow_forward: Downsampled")

        if param.get("SKIP_MESHING", False):
            slack_message(":exclamation: Skip meshing as instructed")
            return

        vol = CloudVolume(seg_cloudpath)
        if mesh_mip not in vol.available_mips:
            mesh_mip = max(vol.available_mips)
        slack_message("Mesh at resolution: {}".format(vol.scales[mesh_mip]['key']))

        tasks = tc.create_meshing_tasks(seg_cloudpath, mip=mesh_mip, simplification=simplification, max_simplification_error=max_simplification_error, shape=Vec(256, 256, 256))
        for t in tasks:
            queue.put(t.payload())
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

        for c in v:
            if c.mip_level() == 0:
                x, y, z = c.coordinate()
                min_id = layer << layer_offset | x << x_offset | y << y_offset | z << z_offset
                max_id = min_id + chunk_voxels
                prefix = commonprefix([str(min_id), str(max_id)])
                if len(prefix) == 0:
                    raise NotImplementedError("No common prefix, need to split the range")
                t = MeshManifestTask(layer_path=seg_cloudpath, prefix=str(prefix))
                queue.put(t.payload())

        check_queue("igneous")
        slack_message(":arrow_forward: Manifest genrated")
        queue.close()
        #tc.create_downsampling_tasks(tq, seg_cloudpath, mip=5, fill_missing=True, preserve_chunk_size=True)
        #check_queue(tq)


