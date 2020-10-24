from functools import wraps

def check_queue(queue):
    import requests
    from time import sleep
    from slack_message import slack_message
    totalTries = 5
    nTries = totalTries
    count = 0
    while True:
        sleep(5)
        ret = requests.get("http://rabbitmq:15672/api/queues/%2f/{}".format(queue), auth=('guest', 'guest'))
        if not ret.ok:
            raise RuntimeError("Cannot connect to rabbitmq management interface")
        queue_status = ret.json()
        nTasks = queue_status["messages"]
        print("Tasks left: {}".format(nTasks))

        count += 1
        if count % 60 == 0:
            slack_message("{} tasks remain in queue {}".format(nTasks, queue))

        if nTasks == 0:
            nTries -= 1
        else:
            nTries = totalTries
        if nTries == 0:
            return


def mount_secrets(func):
    @wraps(func)
    def inner(*args, **kwargs):
        import os
        from airflow.models import Variable
        cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
        if not os.path.exists(cv_secrets_path):
            os.makedirs(cv_secrets_path)

        inner_param = Variable.get("param", deserialize_json=True)
        mount_secrets = inner_param.get("MOUNT_SECRETES", [])

        for k in mount_secrets:
            v = Variable.get(k)
            with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
                value_file.write(v)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            for k in mount_secrets:
                os.remove(os.path.join(cv_secrets_path, k))

    return inner


def kombu_tasks(create_tasks):
    import tenacity

    retry = tenacity.retry(
      reraise=True,
      stop=tenacity.stop_after_attempt(10),
      wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
    )


    @retry
    def submit_message(queue, payload):
        queue.put(payload)


    @wraps(create_tasks)
    def inner(*args, **kwargs):
        from airflow import configuration
        from kombu import Connection
        from google_api_helper import ramp_up_cluster, ramp_down_cluster
        from slack_message import slack_message
        broker = configuration.get('celery', 'BROKER_URL')

        try:
            tasks = create_tasks(*args, **kwargs)
            if not tasks:
                slack_message("No tasks submitted by {}".format(create_tasks.__name__))
                return

            with Connection(broker, connect_timeout=60) as conn:
                queue = conn.SimpleQueue("igneous")
                for t in tasks:
                    submit_message(queue, t.payload())
                queue.close()

            target_size = (1+len(tasks)//32)
            ramp_up_cluster("igneous", min(target_size, 10) , target_size)
            check_queue("igneous")
            ramp_down_cluster("igneous", 0)

            slack_message("All igneous tasks submitted by {} finished".format(create_tasks.__name__))

        except Exception as e:
            slack_message("Failed to submit igneous tasks using {}".format(create_tasks.__name__))
            raise e

    return inner


def dataset_resolution(path, mip=0):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path, mip=mip)
    return vol.resolution.tolist()

def cv_has_data(path, mip=0):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path, mip=mip)
    return vol.image.has_data(mip)

def cv_scale_with_data(path):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path)
    for m in vol.available_mips:
        if vol.image.has_data(m):
            return m, vol.scales[m]['resolution']

def isotropic_mip(path):
    from math import log2
    from cloudvolume import CloudVolume
    vol = CloudVolume(path)
    return int(log2(vol.resolution[2]/vol.resolution[0]))

def mip_for_mesh_and_skeleton(path):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path)
    mip = isotropic_mip(path)
    if mip not in vol.available_mips:
        mip = max(vol.available_mips)

    return mip

def check_cloud_path_empty(path):
    import traceback
    from cloudvolume import Storage
    from slack_message import slack_message
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

@mount_secrets
def create_info(stage, param):
    import os
    from time import strftime
    from cloudvolume import CloudVolume
    from airflow.models import Variable
    from slack_message import slack_message, slack_userinfo
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

    param["CHUNKMAP_OUTPUT"] = os.path.join(param["SCRATCH_PATH"], stage, "chunkmap")
    Variable.set("param", param, serialize_json=True)
    slack_message(""":exclamation: Write the map from chunked segments to real segments to `{}`.""".format(param["CHUNKMAP_OUTPUT"]))

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


def upload_json(path, filename, content):
    from cloudvolume import Storage
    with Storage(path) as storage:
        storage.put_json(filename, content)


def get_atomic_files_job(v, param, prefix):
    from cloudvolume import Storage
    content = b''
    with Storage(param["SCRATCH_PATH"]) as storage:
        for c in v:
            if c.mip_level() != 0:
                continue
            tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
            content += storage.get_file('{}_{}.data'.format(prefix, tag))

    return content


def get_files_job(v, param, prefix):
    from cloudvolume import Storage
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


@mount_secrets
@kombu_tasks
def downsample_for_meshing(seg_cloudpath, mask):
    import igneous.task_creation as tc
    from slack_message import slack_message
    mip, _ = cv_scale_with_data(seg_cloudpath)
    tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=mip, fill_missing=True, mask=mask, num_mips=isotropic_mip(seg_cloudpath), preserve_chunk_size=True)
    slack_message(":arrow_forward: Start downsampling `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks
def downsample(seg_cloudpath):
    import igneous.task_creation as tc
    from slack_message import slack_message
    mip, _ = cv_scale_with_data(seg_cloudpath)
    tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=mip, fill_missing=True, preserve_chunk_size=True)
    slack_message(":arrow_forward: Start downsampling `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks
def mesh(seg_cloudpath, mesh_quality):
    import igneous.task_creation as tc
    from cloudvolume.lib import Vec
    from cloudvolume import CloudVolume
    from slack_message import slack_message
    mesh_mip = mip_for_mesh_and_skeleton(seg_cloudpath)
    simplification = True
    max_simplification_error = 40
    if mesh_quality == "PERFECT":
        simplification = False
        max_simplification_error = 0
        mesh_mip = 0

    vol = CloudVolume(seg_cloudpath)

    slack_message("Mesh at resolution: {}".format(vol.scales[mesh_mip]['key']))

    tasks = tc.create_meshing_tasks(seg_cloudpath, mip=mesh_mip, simplification=simplification, max_simplification_error=max_simplification_error, shape=Vec(256, 256, 256))
    slack_message(":arrow_forward: Start meshing `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))

    return tasks


@mount_secrets
@kombu_tasks
def mesh_manifest(seg_cloudpath, bbox, chunk_size):
    from igneous.tasks import MeshManifestTask
    from chunk_iterator import ChunkIterator
    from os.path import commonprefix
    from slack_message import slack_message
    #FIXME: should reuse the function in segmentation scripts
    layer = 1
    bits_per_dim = 10
    n_bits_for_layer_id = 8

    layer_offset = 64 - n_bits_for_layer_id
    x_offset = layer_offset - bits_per_dim
    y_offset = x_offset - bits_per_dim
    z_offset = y_offset - bits_per_dim

    chunk_voxels = 1 << (64-n_bits_for_layer_id-bits_per_dim*3)

    v = ChunkIterator(bbox, chunk_size)

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
                prefix_list.append(prefix)
            else:
                prefix_list.append(prefix1)
                prefix_list.append(prefix2)

    task_list = []
    tasks = []
    for p in sorted(prefix_list, key=len):
        if any(p.startswith(s) for s in task_list):
            print("Already considered, skip {}".format(p))
            continue
        task_list.append(p)
        t = MeshManifestTask(layer_path=seg_cloudpath, prefix=str(p))
        tasks.append(t)

    slack_message(":arrow_forward: Generating mesh manifest for `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks
def create_skeleton_fragments(seg_cloudpath, teasar_param):
    import igneous.task_creation as tc
    from cloudvolume.lib import Vec
    from slack_message import slack_message
    skeleton_mip = mip_for_mesh_and_skeleton(seg_cloudpath)
    tasks = tc.create_skeletonizing_tasks(seg_cloudpath, mip=skeleton_mip,
                shape=Vec(256, 256, 256),
                sharded=True, # Generate (true) concatenated .frag files (False) single skeleton fragments
                spatial_index=True, # Generate a spatial index so skeletons can be queried by bounding box
                info=None, # provide a cloudvolume info file if necessary (usually not)
                fill_missing=True, # Use zeros if part of the image is missing instead of raising an error
                # see Kimimaro's documentation for the below parameters
                teasar_params=teasar_param,
                object_ids=None, # Only skeletonize these ids
                mask_ids=None, # Mask out these ids
                fix_branching=True, # (True) higher quality branches at speed cost
                fix_borders=True, # (True) Enable easy stitching of 1 voxel overlapping tasks
                dust_threshold=1000, # Don't skeletonize below this physical distance
                progress=False, # Show a progress bar
                parallel=1, # Number of parallel processes to use (more useful locally)
            )
    slack_message(":arrow_forward: Creating skeleton fragments for `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks
def merge_skeleton_fragments(seg_cloudpath):
    import igneous.task_creation as tc
    from slack_message import slack_message
    tasks = tc.create_sharded_skeleton_merge_tasks(seg_cloudpath,
                dust_threshold=1000,
                tick_threshold=3500,
                preshift_bits=9,
                minishard_bits=4,
                shard_bits=11,
                minishard_index_encoding='gzip', # or None
                data_encoding='gzip', # or None
            )
    slack_message(":arrow_forward: Merging skeleton fragments for `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


def downsample_and_mesh(param):
    import os
    seg_cloudpath = param["SEG_PATH"]
    ws_cloudpath = param["WS_PATH"]
    downsample_for_meshing(seg_cloudpath, param.get("SIZE_THRESHOLDED_MESH", False))
    mesh(seg_cloudpath, param.get("MESH_QUALITY", "NORMAL"))
    mesh_manifest(seg_cloudpath, param["BBOX"], param["CHUNK_SIZE"])
    create_skeleton_fragments(seg_cloudpath, param.get("TEASAR_PARAMS", {'scale':10, 'const': 10}))
    merge_skeleton_fragments(seg_cloudpath)
    downsample(seg_cloudpath)
    downsample(ws_cloudpath)
    downsample(os.path.join(seg_cloudpath, "size_map"))


@mount_secrets
@kombu_tasks
def submit_igneous_tasks():
    from airflow.models import Variable
    from slack_message import slack_message
    python_string = Variable.get("python_string")

    exec(python_string, globals())

    if "submit_tasks" not in globals() or not callable(globals()["submit_tasks"]):
        slack_message(":exclamation:*Error* cannot find the submit_tasks function")
        return

    tasks = globals()["submit_tasks"]()

    if not tasks:
        return

    if len(tasks) > 100000:
        slack_message(":exclamation:*Error* too many ({}) tasks, bail".format(len(tasks)))
        raise

    slack_message(":arrow_forward: submit {} igneous tasks".format(len(tasks)))
    return tasks


