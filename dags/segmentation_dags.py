from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.weight_rule import WeightRule
from airflow.models import Variable

from chunkiterator import ChunkIterator

from slack_message import slack_message, task_start_alert, task_done_alert, task_retry_alert
from segmentation_op import composite_chunks_batch_op, composite_chunks_overlap_op, composite_chunks_wrap_op, remap_chunks_batch_op
from helper_ops import slack_message_op, scale_up_cluster_op, scale_down_cluster_op, wait_op, mark_done_op, reset_flags_op, reset_cluster_op, placeholder_op

from param_default import param_default, default_args, CLUSTER_1_CONN_ID, CLUSTER_2_CONN_ID
from igneous_and_cloudvolume import create_info, downsample_and_mesh, get_files_job, get_atomic_files_job, dataset_resolution
from igneous_ops import create_igneous_ops
import numpy as np
import json
import urllib
from collections import OrderedDict

def generate_ng_payload(param):
    ng_resolution = dataset_resolution(param["SEG_PATH"])
    seg_resolution = ng_resolution
    layers = OrderedDict()
    if "IMAGE_PATH" in param:
        layers["img"] = {
            "source": "precomputed://"+param["IMAGE_PATH"],
            "type": "image"
        }
        if "IMAGE_SHADER" in param:
            layers["img"]["shader"] = param["IMAGE_SHADER"]

        ng_resolution = dataset_resolution(param["IMAGE_PATH"])

    layers["aff"] = {
        "source": "precomputed://"+param["AFF_PATH"],
        "shader": param.get("AFF_SHADER", "void main() {\n  float r = toNormalized(getDataValue(0));\n  float g = toNormalized(getDataValue(1));\n  float b = toNormalized(getDataValue(2)); \n  emitRGB(vec3(r,g,b));\n}"),
        "type": "image",
        "visible": False
    }

    if "SEM_PATH" in param:
        layers["sem"] = {
            "source": "precomputed://"+param["SEM_PATH"],
            "type": "segmentation",
            "visible": False
        }

    layers["ws"] = {
        "source": "precomputed://"+param["WS_PATH"],
        "type": "segmentation",
        "visible": False
    }

    layers["seg"] = {
        "source": "precomputed://"+param["SEG_PATH"],
        "type": "segmentation"
    }

    if "GT_PATH" in param:
        layers["gt"] = {
            "source": "precomputed://"+param["GT_PATH"],
            "type": "segmentation"
        }

    layers["size"] = {
        "source": "precomputed://"+param["SEG_PATH"]+"/size_map",
        "type": "image"
    }

    bbox = param["BBOX"]

    scale = [seg_resolution[i]/ng_resolution[i] for i in range(3)]
    center = [(bbox[i]+bbox[i+3])/2*scale[i] for i in range(3)]

    navigation = {
        "pose": {
            "position": {
                "voxelSize": ng_resolution,
                "voxelCoordinates": center
            }
        },
        "zoomFactor": 4
    }

    payload = OrderedDict([("layers", layers),("navigation", navigation),("showSlices", False),("layout", "xy-3d")])
    return payload

def generate_link(param, broadcast, **kwargs):
    ng_host = param.get("NG_HOST", "https://neuromancer-seung-import.appspot.com")
    payload = generate_ng_payload(param)

    if not param.get("SKIP_AGG", False):
        seglist = Variable.get("topsegs")
        payload["layers"]["seg"]["hiddenSegments"] = seglist.split(' ')

    url = "<{host}/#!{payload}|*view the results in neuroglancer*>".format(
        host=ng_host,
        payload=urllib.parse.quote(json.dumps(payload)))
    slack_message(url, broadcast=broadcast)


dag_manager = DAG("segmentation", default_args=default_args, schedule_interval=None)

dag = dict()

dag["ws"] = DAG("watershed", default_args=default_args, schedule_interval=None)

dag["agg"] = DAG("agglomeration", default_args=default_args, schedule_interval=None)

dag["cs"] = DAG("contact_surface", default_args=default_args, schedule_interval=None)

dag_ws = dag["ws"]
dag_agg = dag["agg"]
dag_cs = dag["cs"]

Variable.setdefault("param", param_default, deserialize_json=True)
param = Variable.get("param", deserialize_json=True)
image = param["WORKER_IMAGE"]

for p in ["SCRATCH", "WS", "SEG"]:
    path = "{}_PATH".format(p)
    if path not in param:
        param[path] = param["{}_PREFIX".format(p)]+param["NAME"]


def confirm_dag_run(context, dag_run_obj):
    skip_flag = context['params']['skip_flag']
    op = context['params']['op']
    if param.get(skip_flag, False):
        slack_message(":exclamation: Skip {op}".format(op=op))
    else:
        return dag_run_obj


def process_composite_tasks(c, cm, top_mip, params):
    local_batch_mip = batch_mip
    if top_mip < batch_mip:
        local_batch_mip = top_mip

    if c.mip_level() < local_batch_mip:
        return

    short_queue = "atomic"
    long_queue = "composite"

    composite_queue = short_queue if c.mip_level() < high_mip else long_queue+"_"+str(c.mip_level())

    top_tag = str(top_mip)+"_0_0_0"
    tag = str(c.mip_level()) + "_" + "_".join([str(i) for i in c.coordinate()])
    for stage, op in [("ws", "ws"), ("agg", "me"), ("cs", "cs")]:
        if c.mip_level() > local_batch_mip:
            generate_chunks[stage][c.mip_level()][tag]=composite_chunks_wrap_op(image, dag[stage], cm, composite_queue, tag, stage, op, params)
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][c.mip_level()][tag])
        elif c.mip_level() == local_batch_mip:
            generate_chunks[stage]["batch"][tag]=composite_chunks_batch_op(image, dag[stage], cm, short_queue, local_batch_mip-1, tag, stage, op, params)
            generate_chunks[stage][c.mip_level()][tag]=composite_chunks_wrap_op(image, dag[stage], cm, composite_queue, tag, stage, op, params)
            generate_chunks[stage]["batch"][tag] >> generate_chunks[stage][c.mip_level()][tag]
            slack_ops[stage][c.mip_level()].set_upstream(generate_chunks[stage][c.mip_level()][tag])
            if stage != "cs":
                remap_chunks[stage][tag]=remap_chunks_batch_op(image, dag[stage], cm, short_queue, local_batch_mip, tag, stage, op, params)
                slack_ops[stage]["remap"].set_upstream(remap_chunks[stage][tag])
                generate_chunks[stage][top_mip][top_tag].set_downstream(remap_chunks[stage][tag])
            init[stage].set_downstream(generate_chunks[stage]["batch"][tag])
        if params.get('OVERLAP_MODE', False) and c.mip_level() == overlap_mip and stage == 'agg':
            overlap_chunks[tag] = composite_chunks_overlap_op(image, dag[stage], cm, composite_queue, tag, params)
            for n in c.neighbours():
                n_tag = str(n.mip_level()) + "_" + "_".join([str(i) for i in n.coordinate()])
                if n_tag in generate_chunks[stage][c.mip_level()]:
                    overlap_chunks[tag].set_upstream(generate_chunks[stage][n.mip_level()][n_tag])
                    if n_tag != tag:
                        overlap_chunks[n_tag].set_upstream(generate_chunks[stage][c.mip_level()][tag])
            #slack_ops[stage][c.mip_level()].set_downstream(overlap_chunks[tag])
            slack_ops[stage]['overlap'].set_upstream(overlap_chunks[tag])

    if c.mip_level() < top_mip:
        parent_coord = [i//2 for i in c.coordinate()]
        parent_tag = str(c.mip_level()+1) + "_" + "_".join([str(i) for i in parent_coord])
        for stage in ["ws", "agg", "cs"]:
            if params.get("OVERLAP_MODE", False) and c.mip_level() == overlap_mip and stage == "agg":
                overlap_chunks[tag].set_downstream(generate_chunks[stage][c.mip_level()+1][parent_tag])
            else:
                generate_chunks[stage][c.mip_level()][tag].set_downstream(generate_chunks[stage][c.mip_level()+1][parent_tag])


def generate_batches(param):
    v = ChunkIterator(param["BBOX"], param["CHUNK_SIZE"])
    top_mip = v.top_mip_level()
    batch_mip = 3
    batch_chunks = []
    high_mip_chunks = []
    current_mip = top_mip
    mip_tasks = 0
    if top_mip > batch_mip:
        for c in v:
            if c.mip_level() > batch_mip:
                if current_mip != c.mip_level():
                    if current_mip > batch_mip and mip_tasks > 50:
                        batch_mip = c.mip_level()
                    current_mip = c.mip_level()
                    mip_tasks = 1
                else:
                    mip_tasks += 1
                high_mip_chunks.append(c)
            elif c.mip_level() < batch_mip:
                break
            elif c.mip_level() == batch_mip:
                batch_chunks.append(ChunkIterator(param["BBOX"], param["CHUNK_SIZE"], start_from = [batch_mip]+c.coordinate()))
    else:
        batch_chunks=[v]

    return high_mip_chunks, batch_chunks


def get_atomic_files(param, prefix):
    from joblib import Parallel, delayed

    high_mip_chunks, batch_chunks = generate_batches(param)
    contents = Parallel(n_jobs=-2)(delayed(get_atomic_files_job)(sv, param, prefix) for sv in batch_chunks)

    content = b''
    for c in contents:
        content+=c

    return content

def classify_segmentats(param):
    prefix = "agg/info/semantic_labels"
    content = get_files(param, prefix)
    sem_type = [('s', np.uint64), ('dendrite', np.uint64), ('axon', np.uint64), ('glia', np.uint64)]
    data = np.frombuffer(content, dtype=sem_type)
    segs = set()
    for d in data:
        if d['glia'] > d['dendrite'] and d['glia'] > d['axon']:
            continue
        else:
            segs.add(int(d['s']))

    return segs

def compare_segmentation(param, **kwargs):
    from io import BytesIO
    import os
    from collections import defaultdict
    from evaluate_segmentation import read_chunk, evaluate_rand, evaluate_voi, find_large_diff
    from igneous_and_cloudvolume import upload_json
    from airflow import configuration as conf
    segs = classify_segmentats(param)
    prefix = "agg/evaluation/evaluation"
    content = get_atomic_files(param, prefix)
    f = BytesIO(content)
    s_i = defaultdict(int)
    t_j = defaultdict(int)
    p_ij = defaultdict(lambda: defaultdict(int))
    payload = generate_ng_payload(param)
    payload['layers']['size']['visible'] = False
    while True:
        if not read_chunk(f, s_i, t_j, p_ij):
            break
    rand_split, rand_merge = evaluate_rand(s_i, t_j, p_ij)
    voi_split, voi_merge = evaluate_voi(s_i, t_j, p_ij)
    seg_pairs = find_large_diff(s_i, t_j, p_ij, segs)
    scores = {
        'rand_split': rand_split,
        'rand_merge': rand_merge,
        'voi_split': voi_split,
        'voi_merge': voi_merge,
    }
    Variable.set("seg_eval", scores, serialize_json=True)
    output = {
        "ng_payload": payload,
        "seg_pairs": seg_pairs
    }
    gs_log_path = conf.get('logging', 'remote_base_log_folder')
    bucket_name = gs_log_path[5:].split('/')[0]

    upload_json("gs://"+os.path.join(bucket_name,"diff"), "{}.json".format(param["NAME"]), output)


def evaluate_results(param, **kwargs):
    if "GT_PATH" not in param:
        return

    diff_server = param.get("DIFF_SERVER", "https://diff-dot-neuromancer-seung-import.appspot.com")

    scores = Variable.get("seg_eval", deserialize_json=True)

    msg = '''*Evaluation against ground truth* `{gt_path}`:
rand split: *{rand_split}*
rand merge: *{rand_merge}*
voi split : *{voi_split}*
voi merge : *{voi_merge}*
seg diff: {url}
'''.format(
    gt_path=param["GT_PATH"],
    rand_split=round(abs(scores['rand_split']),3),
    rand_merge=round(abs(scores['rand_merge']),3),
    voi_split=round(abs(scores['voi_split']),3),
    voi_merge=round(abs(scores['voi_merge']),3),
    url="{}/{}".format(diff_server, param["NAME"])
    )
    slack_message(msg, broadcast=True)


def plot_histogram(data, title, xlabel, ylabel, fn):
    import math
    import matplotlib.pyplot as plt
    plt.clf()
    min_bin = math.floor(math.log10(max(1,min(data))))
    max_bin = math.ceil(math.log10(max(data)))
    plt.hist(data, bins=np.logspace(min_bin, max_bin, (max_bin-min_bin)+1))
    plt.xscale('log')
    plt.yscale('log')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.savefig(fn)


def contact_surfaces(param):
    prefix = "cs/cs/complete_cs"
    content = get_files(param, prefix)
    cs_type = [('s1', np.uint64), ('s2', np.uint64), ('sumx', np.uint64), ('sumy', np.uint64),('sumz', np.uint64), ('size', np.uint64), ('sizex', np.uint64), ('sizey', np.uint64),('sizez', np.uint64),
    ('minx', np.int64),('miny', np.int64),('minz', np.int64),('maxx', np.int64),('maxy', np.int64),('maxz', np.int64)]

    data = np.frombuffer(content, dtype=cs_type)

    title = "Distribution of the contact sizes"
    xlabel = "Number of voxels in the contact surface"
    ylabel = "Number of contact surfaces"
    plot_histogram(data['size'], title, xlabel, ylabel, '/tmp/hist.png')

    order = np.argsort(data['size'])[::-1]

    msg = '''*Finished extracting contact surfaces in* `{seg_path}`:
number of contact surfaces: *{size_cs}*
average contact area: *{mean_cs}*
maximal contact area: *{max_cs}*
output location: `{url}`
'''.format(
    seg_path=param["SEG_PATH"],
    size_cs=len(data),
    mean_cs=int(np.mean(data['size'])),
    max_cs=data[order[0]]['size'],
    url="{}/{}".format(param["SCRATCH_PATH"], "cs/cs")
    )
    slack_message(msg, attachment='/tmp/hist.png')

    xlabel = "Number of boundary voxels in the x direction"
    plot_histogram(data['sizex'], title, xlabel, ylabel, '/tmp/histx.png')
    slack_message("", attachment='/tmp/histx.png')
    xlabel = "Number of boundary voxels in the y direction"
    plot_histogram(data['sizey'], title, xlabel, ylabel, '/tmp/histy.png')
    slack_message("", attachment='/tmp/histy.png')
    xlabel = "Number of boundary voxels in the z direction"
    plot_histogram(data['sizez'], title, xlabel, ylabel, '/tmp/histz.png')
    slack_message("", attachment='/tmp/histz.png')


def get_files(param, prefix):
    from joblib import Parallel, delayed

    high_mip_chunks, batch_chunks = generate_batches(param)
    print("get {} high mip files".format(len(high_mip_chunks)))
    content = get_files_job(high_mip_chunks, param, prefix)
    print("get lower mip files")
    contents = Parallel(n_jobs=-2)(delayed(get_files_job)(sv, param, prefix) for sv in batch_chunks)

    for c in contents:
        content+=c

    return content


def process_infos(param, **kwargs):
    dt_count = np.dtype([('segid', np.uint64), ('count', np.uint64)])
    prefix = "agg/info/seg_size"
    content = get_files(param, prefix)
    data = np.frombuffer(content, dtype=dt_count)
    title = "Distribution of the segment sizes"
    xlabel = "Number of voxels in the segments"
    ylabel = "Number of segments"
    plot_histogram(data['count'], title, xlabel, ylabel, '/tmp/hist.png')
    order = np.argsort(data['count'])[::-1]
    ntops = min(20,len(data))
    msg = '''*Agglomeration Finished*
*{nseg}* segments (*{nsv}* voxels)

Largest segments:
{top20list}'''.format(
    nseg=len(data),
    nsv=np.sum(data['count']),
    top20list="\n".join("id: {} ({})".format(data[order[i]][0], data[order[i]][1]) for i in range(ntops))
    )
    slack_message(msg, attachment='/tmp/hist.png')
    Variable.set("topsegs", " ".join(str(int(data[order[i]][0])) for i in range(ntops)))


if "BBOX" in param and "CHUNK_SIZE" in param: #and "AFF_MIP" in param:
    data_bbox = param["BBOX"]

    chunk_size = param["CHUNK_SIZE"]

    v = ChunkIterator(data_bbox, chunk_size)
    top_mip = v.top_mip_level()
    batch_mip = param.get("BATCH_MIP", 3)
    high_mip = param.get("HIGH_MIP", 5)
    if param.get("OVERLAP_MODE", False):
        overlap_mip = param.get("OVERLAP_MIP", batch_mip)
    local_batch_mip = batch_mip
    aux_queue = "atomic" if top_mip < high_mip else "composite_"+str(top_mip)


    #data_bbox = [126280+256, 64280+256, 20826-200, 148720-256, 148720-256, 20993]
    starting_msg ='''*Start Segmenting {name}*
    Affinity map: `{aff}`
    Affinity mip level: {mip}
    Bounding box: [{bbox}]'''.format(
        name = param["NAME"],
        aff = param["AFF_PATH"],
        bbox = ", ".join(str(x) for x in param["BBOX"]),
        mip = param.get("AFF_MIP",0)
    )

    ending_msg = '''*Finish Segmenting {name}*
    Watershed layer: `{ws}`
    Segmentation Layer: `{seg}`'''.format(
        name = param["NAME"],
        ws = param["WS_PATH"],
        seg = param["SEG_PATH"]
    )

    no_rescale_msg = ":exclamation: Cannot rescale cluster"
    rescale_message = ":heavy_check_mark: Rescaled cluster {} to {} instances"

    starting_op = slack_message_op(dag_manager, "start", starting_msg)
    ending_op = slack_message_op(dag_manager, "end", ending_msg)

    reset_flags = reset_flags_op(dag_manager, param)

    init = dict()

    init["ws"] = PythonOperator(
        task_id = "Init_Watershed",
        python_callable=create_info,
        op_args = ["ws", param],
        default_args=default_args,
        on_success_callback=task_start_alert,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        dag=dag["ws"],
        queue = "manager"
    )

    init["agg"] = PythonOperator(
        task_id = "Init_Agglomeration",
        python_callable=create_info,
        op_args = ["agg", param],
        default_args=default_args,
        on_success_callback=task_start_alert,
        on_retry_callback=task_retry_alert,
        weight_rule=WeightRule.ABSOLUTE,
        dag=dag["agg"],
        queue = "manager"
    )

    init['cs'] = slack_message_op(dag['cs'], "init_cs", ":arrow_forward: Start extracting contact surfaces")

    generate_chunks = {
        "ws": {},
        "agg": {},
        "cs": {}
    }

    overlap_chunks = {}

    remap_chunks = {
        "ws": {},
        "agg": {}
    }

    slack_ops = {
        "ws": {},
        "agg": {},
        "cs": {}
    }

    scaling_ops = {
        "ws": {},
        "agg": {},
        "cs": {}
    }

    triggers = dict()
    wait = dict()
    mark_done = dict()

    if param.get("SKIP_WS", False):
        triggers["ws"] = slack_message_op(dag_manager, "skip_ws", ":exclamation: Skip watershed")
        wait["ws"] = placeholder_op(dag_manager, "ws_done")
    else:
        triggers["ws"] = TriggerDagRunOperator(
            task_id="trigger_ws",
            trigger_dag_id="watershed",
            queue="manager",
            dag=dag_manager
        )
        wait["ws"] = wait_op(dag_manager, "ws_done")

    mark_done["ws"] = mark_done_op(dag["ws"], "ws_done")

    if param.get("SKIP_AGG", False):
        triggers["agg"] = slack_message_op(dag_manager, "skip_agg", ":exclamation: Skip agglomeration")
        wait["agg"] = placeholder_op(dag_manager, "agg_done")
    else:
        triggers["agg"] = TriggerDagRunOperator(
            task_id="trigger_agg",
            trigger_dag_id="agglomeration",
            queue="manager",
            dag=dag_manager
        )
        wait["agg"] = wait_op(dag_manager, "agg_done")

    mark_done["agg"] = mark_done_op(dag["agg"], "agg_done")

    check_seg = PythonOperator(
        task_id="Check_Segmentation",
        python_callable=process_infos,
        provide_context=True,
        op_args=[param],
        default_args=default_args,
        on_success_callback=task_done_alert,
        on_retry_callback=task_retry_alert,
        dag=dag_agg,
        queue=aux_queue
    )

    aux_agg_tasks = [check_seg]

    summary_cs = PythonOperator(
        task_id="CS_Summary",
        python_callable=contact_surfaces,
        op_args=[param],
        on_success_callback=task_done_alert,
        on_retry_callback=task_retry_alert,
        default_args=default_args,
        dag=dag_cs,
        queue=aux_queue
    )

    if "GT_PATH" in param:
        comp_seg_task = PythonOperator(
            task_id = "Compare_Segmentation",
            python_callable=compare_segmentation,
            provide_context=True,
            op_args=[param,],
            default_args=default_args,
            dag=dag_agg,
            on_success_callback=task_done_alert,
            on_retry_callback=task_retry_alert,
            queue=aux_queue
        )
        aux_agg_tasks.append(comp_seg_task)

    cm = ["param"]
    if "MOUNT_SECRETS" in param:
        cm += param["MOUNT_SECRETS"]

    if top_mip < batch_mip:
        local_batch_mip = top_mip

    if top_mip == batch_mip:
        param["OVERLAP_MODE"] = False

    if param.get("OVERLAP_MODE", False) and top_mip > overlap_mip:
        slack_ops['agg']['overlap'] = slack_message_op(dag['agg'], "overlap_"+str(overlap_mip), ":heavy_check_mark: {} MIP {} finished".format("overlapped agglomeration at", overlap_mip))

    for c in v:
        if c.mip_level() < local_batch_mip:
            break
        else:
            for k in ["ws","agg","cs"]:
                generate_chunks[k]["batch"] = {}
                if c.mip_level() not in generate_chunks[k]:
                    generate_chunks[k][c.mip_level()] = {}

                if c.mip_level() not in slack_ops[k]:
                    slack_ops[k][c.mip_level()] = slack_message_op(dag[k], k+str(c.mip_level()), ":heavy_check_mark: {}: MIP {} finished".format(k, c.mip_level()))
                    if c.mip_level() == local_batch_mip and k != "cs":
                        slack_ops[k]["remap"] = slack_message_op(dag[k], "remap_{}".format(k), ":heavy_check_mark: {}: Remaping finished".format(k))
                        slack_ops[k]["remap"] >> mark_done[k]
            process_composite_tasks(c, cm, top_mip, param)

    cluster1_size = len(remap_chunks["ws"])


    if cluster1_size >= 100:
        reset_cluster_after_ws = reset_cluster_op(dag['ws'], "ws", CLUSTER_1_CONN_ID, 20)
        slack_ops['ws']['remap'] >> reset_cluster_after_ws


    scaling_global_start = scale_up_cluster_op(dag_manager, "global_start", CLUSTER_1_CONN_ID, 20, cluster1_size)

    scaling_global_finish = scale_down_cluster_op(dag_manager, "global_finish", CLUSTER_1_CONN_ID, 0)

    scaling_cs_start = scale_up_cluster_op(dag_cs, "cs_start", CLUSTER_1_CONN_ID, 20, cluster1_size)

    scaling_cs_finish = scale_down_cluster_op(dag_cs, "cs_finish", CLUSTER_1_CONN_ID, 0)

    scaling_cs_start >> init['cs']
    slack_ops['cs'][top_mip] >> summary_cs >> scaling_cs_finish
    slack_ops['agg'][top_mip] >> check_seg >> mark_done['agg']
    if "GT_PATH" in param:
        slack_ops['agg']['remap'] >> comp_seg_task >> mark_done['agg']


    igneous_tasks = create_igneous_ops(param, dag_manager)

    scaling_igneous_finish = scale_down_cluster_op(dag_manager, "igneous_finish", "igneous", 0)

    starting_op >> reset_flags >> triggers["ws"] >> wait["ws"] >> triggers["agg"] >> wait["agg"] >> scaling_global_finish >> igneous_tasks[0]
    igneous_tasks[-1] >> ending_op
    reset_flags >> scaling_global_start
    igneous_tasks[-1]>> scaling_igneous_finish

    nglink_task = PythonOperator(
        task_id = "Generate_neuroglancer_link",
        provide_context=True,
        python_callable=generate_link,
        op_args = [param, True],
        default_args=default_args,
        dag=dag_manager,
        queue = "manager"
    )
    igneous_tasks[-1] >> nglink_task >> ending_op
    if "GT_PATH" in param:
        evaluation_task = PythonOperator(
            task_id = "Evaluate_Segmentation",
            provide_context=True,
            python_callable=evaluate_results,
            op_args = [param,],
            default_args=default_args,
            dag=dag_manager,
            queue = "manager"
        )
        igneous_tasks[-1] >> evaluation_task >> ending_op


    if min(high_mip, top_mip) - batch_mip > 2:
        for stage in ["ws", "agg", "cs"]:
            dsize = len(generate_chunks[stage][batch_mip+2])*2
            scaling_ops[stage]["extra_down"] = scale_down_cluster_op(dag[stage], stage, CLUSTER_1_CONN_ID, dsize)
            scaling_ops[stage]["extra_down"].set_upstream(slack_ops[stage][batch_mip+1])

    if top_mip >= high_mip:
        for stage in ["ws", "agg", "cs"]:
            scaling_ops[stage]["down"] = scale_down_cluster_op(dag[stage], stage, CLUSTER_1_CONN_ID, 0)
            scaling_ops[stage]["down"].set_upstream(slack_ops[stage][high_mip-1])


            cluster2_size = max(1, len(generate_chunks[stage][high_mip])//8)
            scaling_ops[stage]["up_long"] = scale_up_cluster_op(dag[stage], stage+"_long", CLUSTER_2_CONN_ID, 2, cluster2_size)

            for k in generate_chunks[stage][high_mip-1]:
                scaling_ops[stage]["up_long"].set_upstream(generate_chunks[stage][high_mip-1][k])

            scaling_ops[stage]["down_long"] = scale_down_cluster_op(dag[stage], stage+"_long", CLUSTER_2_CONN_ID, 0)
            if stage == "cs":
                scaling_ops[stage]["down_long"].set_upstream(summary_cs)
            elif stage == "agg":
                aux_agg_tasks >> scaling_ops[stage]["down_long"]
            else:
                scaling_ops[stage]["down_long"].set_upstream(slack_ops[stage][top_mip])

    if min(high_mip, top_mip) - batch_mip > 2 or top_mip >= high_mip:
        for stage in ["ws", "agg"]:
            scaling_ops[stage]["up"] = scale_up_cluster_op(dag[stage], stage, CLUSTER_1_CONN_ID, 20, cluster1_size)
            scaling_ops[stage]["up"].set_upstream(slack_ops[stage][top_mip])

