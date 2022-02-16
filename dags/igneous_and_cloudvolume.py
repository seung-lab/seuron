from functools import wraps

def process_worker_messages(ret_queue, agg):
    from slack_message import slack_message
    from kombu.simple import SimpleQueue
    from time import sleep
    import json
    while True:
        try:
            message = ret_queue.get_nowait()
        except SimpleQueue.Empty:
            break
        if agg:
            agg.update(json.loads(message.payload))
        else:
            sleep(1)
            slack_message(f"worker message: {message.payload}")

        message.ack()

def process_worker_errors(err_queue):
    from slack_message import slack_message
    from kombu.simple import SimpleQueue
    import json
    import base64
    err_msg = None
    msg_count = 0
    while True:
        try:
            message = err_queue.get_nowait()
        except SimpleQueue.Empty:
            break
        if not err_msg:
            err_msg = base64.b64decode(message.payload.encode("UTF-8")).decode("UTF-8")
        msg_count += 1
        message.ack()

    if err_msg:
        slack_message(f"{msg_count} worker errors: {err_msg}")


def check_queue(queue, agg=None):
    from airflow import configuration
    import requests
    from time import sleep
    from slack_message import slack_message
    from kombu import Connection
    broker = configuration.get('celery', 'BROKER_URL')
    totalTries = 5
    nTries = totalTries
    count = 0
    with Connection(broker) as conn:
        ret_queue = conn.SimpleQueue(queue+"_ret")
        err_queue = conn.SimpleQueue(queue+"_err")
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
            process_worker_messages(ret_queue, agg)
            process_worker_errors(err_queue)

            if nTasks == 0:
                nTries -= 1
            else:
                nTries = totalTries
            if nTries == 0:
                ret_queue.close()
                return


def mount_secrets(func):
    @wraps(func)
    def inner(*args, **kwargs):
        import os
        from airflow.models import Variable
        from slack_message import slack_message
        cv_secrets_path = os.path.join(os.path.expanduser('~'),".cloudvolume/secrets")
        if not os.path.exists(cv_secrets_path):
            os.makedirs(cv_secrets_path)

        inner_param = Variable.get("param", deserialize_json=True)
        mount_secrets = inner_param.get("MOUNT_SECRETS", [])

        for k in mount_secrets:
            v = Variable.get(k)
            with open(os.path.join(cv_secrets_path, k), 'w') as value_file:
                value_file.write(v)
            slack_message(f"mount secret `{k}` to `{cv_secrets_path}`")
        try:
            return func(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            for k in mount_secrets:
                os.remove(os.path.join(cv_secrets_path, k))

    return inner


def kombu_tasks(cluster_name, init_workers, worker_factor):
    import tenacity

    retry = tenacity.retry(
      reraise=True,
      stop=tenacity.stop_after_attempt(10),
      wait=tenacity.wait_random_exponential(multiplier=0.5, max=60.0),
    )


    @retry
    def submit_message(queue, payload):
        queue.put(payload)


    def extract_payload(msg):
        from taskqueue.queueables import totask
        from taskqueue.lib import jsonify
        if type(msg) is str:
            return msg
        else:
            return jsonify(totask(msg).payload())


    def decorator(create_tasks):
        @wraps(create_tasks)
        def inner(*args, **kwargs):
            from airflow import configuration
            from kombu import Connection
            from google_api_helper import ramp_up_cluster, ramp_down_cluster
            from slack_message import slack_message
            broker = configuration.get('celery', 'BROKER_URL')
            queue_name = cluster_name

            try:
                ret = create_tasks(*args, **kwargs)
                if isinstance(ret, dict):
                    tasks = ret.get('tasks', None)
                    agg = ret.get('aggregator', None)
                else:
                    tasks = ret
                    agg = None

                if not tasks:
                    slack_message("No tasks submitted by {}".format(create_tasks.__name__))
                    return

                try:
                    tasks = list(tasks)
                except TypeError:
                    slack_message("{} must return a list of tasks".format(create_tasks.__name__))
                    return

                with Connection(broker, connect_timeout=60) as conn:
                    queue = conn.SimpleQueue(queue_name)
                    for t in tasks:
                        payload = extract_payload(t)
                        submit_message(queue, payload)
                    queue.close()

                target_size = (1+len(tasks)//worker_factor)
                ramp_up_cluster(cluster_name, min(target_size, init_workers) , target_size)
                check_queue(queue_name, agg)
                ramp_down_cluster(cluster_name, 0)
                if agg:
                    agg.finalize()

                slack_message("All tasks submitted by {} finished".format(create_tasks.__name__))

            except Exception as e:
                slack_message("Failed to submit tasks using {}".format(create_tasks.__name__))
                raise e
            finally:
                ramp_down_cluster(cluster_name, 0)

        return inner

    return decorator


def dataset_resolution(path, mip=0):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path, mip=mip)
    return vol.resolution.tolist()

def cv_has_data(path, mip=0):
    from cloudvolume import CloudVolume
    from slack_message import slack_message
    vol = CloudVolume(path, mip=mip)
    try:
        return vol.image.has_data(mip)
    except NotImplementedError:
        slack_message("CloudVolume does not support has_data for layer `{}`, assume data exists".format(path))
        return True


def cv_scale_with_data(path):
    from cloudvolume import CloudVolume
    from slack_message import slack_message
    vol = CloudVolume(path)
    for m in vol.available_mips:
        try:
            if vol.image.has_data(m):
                return m, vol.scales[m]['resolution']
        except NotImplementedError:
            slack_message("CloudVolume does not support has_data for layer `{}`. You need to explicitly specify the input resolution".format(path))


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
    from cloudfiles import CloudFiles
    from slack_message import slack_message
    try:
        cf = CloudFiles(path)
        obj = next(cf.list(), None)
    except:
        slack_message(""":exclamation:*Error*: Check cloud path failed:
```{}``` """.format(traceback.format_exc()))
        raise

    if obj is not None:
        slack_message(""":exclamation:*Error*: `{}` is not empty""".format(path))
        raise RuntimeError('Path already exist')

@mount_secrets
def commit_info(path, info, provenance):
    from cloudvolume import CloudVolume
    from slack_message import slack_userinfo
    vol = CloudVolume(path, mip=0, info=info)
    vol.provenance.processing.append(provenance)

    try:
        vol.commit_info()
        vol.commit_provenance()
    except:
        slack_message(""":exclamation:*Error*: Cannot commit cloudvolume info to `{}`, check if the bot have write permission.""".format(cv_path))
        raise


def create_info(stage, param, top_mip):
    import os
    from time import strftime
    from cloudvolume import CloudVolume
    from airflow.models import Variable
    from slack_message import slack_message, slack_userinfo

    param["CHUNKMAP_OUTPUT"] = os.path.join(param["SCRATCH_PATH"], stage, "chunkmap")
    if param.get("CHUNKED_AGG_OUTPUT", False):
        param["CHUNKED_SEG_PATH"] = os.path.join(param['SEG_PATH'], f'chunked')

    Variable.set("param", param, serialize_json=True)

    author = slack_userinfo()
    if author is None:
        author = "seuronbot"

    provenance = {
        'method': param,
        'by': author,
        'date': strftime('%Y-%m-%d %H:%M %Z')
    }

    bbox = param["BBOX"]
    resolution = param["AFF_RESOLUTION"]
    cv_chunk_size = param.get("CV_CHUNK_SIZE", [256,256,64])
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

    if stage == 'ws':
        commit_info(param['WS_PATH'], metadata_seg, provenance)
    elif stage == "agg":
        commit_info(param['SEG_PATH'], metadata_seg, provenance)
        if param.get("CHUNKED_AGG_OUTPUT", False):
            slack_message(""":exclamation:Output chunked segmentation to `{}`.""".format(param["CHUNKED_SEG_PATH"]))
            commit_info(param["CHUNKED_SEG_PATH"], metadata_seg, provenance)

        for i in range(top_mip):
            commit_info(os.path.join(param['SEG_PATH'], f'layer_{i+1}'), metadata_seg, provenance)

        cv_path = os.path.join(param["SEG_PATH"], "size_map")
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
        commit_info(os.path.join(param['SEG_PATH'], 'size_map'), metadata_size, provenance)

    if stage == "ws" or param.get("CHUNKED_AGG_OUTPUT", False):
        slack_message(""":exclamation: Write the map from chunked segments to real segments to `{}`.""".format(param["CHUNKMAP_OUTPUT"]))


def upload_json(path, filename, content):
    from cloudfiles import CloudFiles
    cf = CloudFiles(path)
    cf.put_json(filename, content)


def get_atomic_files_job(v, param, prefix):
    from cloudfiles import CloudFiles
    content = b''
    cf = CloudFiles(param["SCRATCH_PATH"])
    for c in v:
        if c.mip_level() != 0:
            continue
        tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
        content += cf[f'{prefix}_{tag}.data']

    return content


def get_files_job(v, param, prefix):
    from cloudfiles import CloudFiles
#    try:
    content = b''
    cf = CloudFiles(param["SCRATCH_PATH"])
    for c in v:
        tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
        content += cf[f'{prefix}_{tag}.data']

    return content

def put_file_job(content, param, prefix):
    from cloudfiles import CloudFiles
#    try:
    cf = CloudFiles(param["SCRATCH_PATH"])
    cf[f'{prefix}_all.data'] = content

    return content

#    except:
#        print("Cannot read all the info files")
#        return None


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=8, worker_factor=32)
def downsample_for_meshing(seg_cloudpath, mask):
    import igneous.task_creation as tc
    from slack_message import slack_message
    mip, _ = cv_scale_with_data(seg_cloudpath)
    tasks = tc.create_downsampling_tasks(seg_cloudpath, mip=mip, fill_missing=False, num_mips=isotropic_mip(seg_cloudpath), preserve_chunk_size=True)
    slack_message(":arrow_forward: Start downsampling `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=8, worker_factor=32)
def downsample(*args):
    import igneous.task_creation as tc
    from slack_message import slack_message
    total_tasks = []
    for seg_cloudpath in args:
        mip, _ = cv_scale_with_data(seg_cloudpath)
        tasks = list(tc.create_downsampling_tasks(seg_cloudpath, mip=mip, fill_missing=False, num_mips=isotropic_mip(seg_cloudpath), preserve_chunk_size=True))
        slack_message(":arrow_forward: Start downsampling `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
        total_tasks += tasks
    return total_tasks


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=8, worker_factor=32)
def mesh(seg_cloudpath, mesh_quality, sharded):
    import igneous.task_creation as tc
    from cloudvolume.lib import Vec
    from cloudvolume import CloudVolume
    from slack_message import slack_message

    if mesh_quality == "PERFECT":
        simplification = False
        max_simplification_error = 0
        mesh_mip = 0
    else:
        mesh_mip = mip_for_mesh_and_skeleton(seg_cloudpath)
        simplification = True
        max_simplification_error = 40

    if sharded:
        spatial_index=True
    else:
        spatial_index=False


    vol = CloudVolume(seg_cloudpath)

    slack_message("Mesh at resolution: {}".format(vol.scales[mesh_mip]['key']))

    tasks = tc.create_meshing_tasks(seg_cloudpath,
                                    mip=mesh_mip,
                                    simplification=simplification,
                                    max_simplification_error=max_simplification_error,
                                    cdn_cache=False,
                                    fill_missing=False,
                                    encoding='precomputed',
                                    spatial_index=spatial_index,
                                    sharded=sharded,
                                    shape=Vec(256, 256, 256))
    slack_message(":arrow_forward: Start meshing `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))

    return tasks


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=8, worker_factor=4)
def merge_mesh_fragments(seg_cloudpath):
    import igneous.task_creation as tc
    from slack_message import slack_message
    tasks = tc.create_sharded_multires_mesh_tasks(seg_cloudpath)
    slack_message(":arrow_forward: Merge mesh fragments `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=4, worker_factor=32)
def mesh_manifest(seg_cloudpath, bbox, chunk_size):
    from igneous.tasks import MeshManifestTask
    from os.path import commonprefix
    from slack_message import slack_message
    import math
    #FIXME: should reuse the function in segmentation scripts
    layer = 1
    bits_per_dim = 10
    n_bits_for_layer_id = 8

    layer_offset = 64 - n_bits_for_layer_id
    x_offset = layer_offset - bits_per_dim
    y_offset = x_offset - bits_per_dim
    z_offset = y_offset - bits_per_dim

    chunk_voxels = 1 << (64-n_bits_for_layer_id-bits_per_dim*3)

    prefix_list = []
    for x in range(math.ceil((bbox[3]-bbox[0])/chunk_size[0])):
        for y in range(math.ceil((bbox[4]-bbox[1])/chunk_size[1])):
            for z in range(math.ceil((bbox[5]-bbox[2])/chunk_size[2])):
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

    tasks = []
    prefix_list = sorted(prefix_list)
    ptask = None
    for p in prefix_list:
        if ptask and p.startswith(ptask):
            print(f"Already considered task {ptask}, skip {p}")
            continue

        ptask = p
        t = MeshManifestTask(layer_path=seg_cloudpath, prefix=str(p))
        tasks.append(t)

    slack_message(":arrow_forward: Generating mesh manifest for `{}`: {} tasks in total".format(seg_cloudpath, len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=8, worker_factor=32)
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
@kombu_tasks(cluster_name="igneous", init_workers=4, worker_factor=32)
def merge_skeleton_fragments(seg_cloudpath):
    import igneous.task_creation as tc
    from slack_message import slack_message
    tasks = tc.create_sharded_skeleton_merge_tasks(seg_cloudpath,
                dust_threshold=1000,
                tick_threshold=3500,
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
    downsample(ws_cloudpath, seg_cloudpath, os.path.join(seg_cloudpath, "size_map"))


@mount_secrets
@kombu_tasks(cluster_name="igneous", init_workers=4, worker_factor=32)
def submit_igneous_tasks():
    from airflow.models import Variable
    from slack_message import slack_message
    python_string = Variable.get("igneous_script")

    exec(python_string, globals())

    if "submit_tasks" not in globals() or not callable(globals()["submit_tasks"]):
        slack_message(":exclamation:*Error* cannot find the submit_tasks function")
        return

    tasks = list(globals()["submit_tasks"]())

    if not tasks:
        return

    if len(tasks) > 1000000:
        slack_message(":exclamation:*Error* too many ({}) tasks, bail".format(len(tasks)))
        raise

    slack_message(":arrow_forward: submitting {} igneous tasks".format(len(tasks)))
    return tasks


@mount_secrets
@kombu_tasks(cluster_name="custom-cpu", init_workers=4, worker_factor=32)
def submit_custom_cpu_tasks():
    from airflow.models import Variable
    from slack_message import slack_message
    python_string = Variable.get("custom_script")

    exec(python_string, globals())

    if "submit_tasks" not in globals() or not callable(globals()["submit_tasks"]):
        slack_message(":exclamation:*Error* cannot find the submit_tasks function")
        return

    if "process_task" not in globals() or not callable(globals()["process_task"]):
        slack_message(":exclamation:*Error* cannot find the process_task function")
        return

    tasks = globals()["submit_tasks"]()

    return tasks


@mount_secrets
@kombu_tasks(cluster_name="custom-gpu", init_workers=4, worker_factor=2)
def submit_custom_gpu_tasks():
    from airflow.models import Variable
    from slack_message import slack_message
    python_string = Variable.get("custom_script")

    exec(python_string, globals())

    if "submit_tasks" not in globals() or not callable(globals()["submit_tasks"]):
        slack_message(":exclamation:*Error* cannot find the submit_tasks function")
        return

    if "process_task" not in globals() or not callable(globals()["process_task"]):
        slack_message(":exclamation:*Error* cannot find the process_task function")
        return

    tasks = globals()["submit_tasks"]()

    return tasks
