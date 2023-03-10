"""Neuroglancer link functions."""
import json
import urllib
from typing import Optional
from collections import OrderedDict


def generate_ng_payload(
    param: dict, center: Optional[tuple[int, int, int]] = None
) -> str:
    """Makes neuroglancer link payloads from mappings between layer names and paths.

    Makes the viewer resolution equal to the "img" layer if it exists in param.
    Otherwise, it takes the first it can find.
    """
    assert len(param) > 0, "empty layer paths"

    layers = OrderedDict()
    param = param.copy()  # for in-place modifications later
    # default to first resolution we can find - update if we find an image
    ng_resolution = dataset_resolution(list(param.values())[0])
    img_path = param.pop("img", None)
    if img_path:
        layers["img"] = {"source": "precomputed://" + img_path, "type": "image"}
        ng_resolution = dataset_resolution(img_path)

    for key in param:
        if key == "NG_HOST":
            continue

        layers[key] = {"source": "precomputed://" + param[key], "type": "segmentation"}

    navigation = {"pose": {"position": {"voxelSize": ng_resolution}}, "zoomFactor": 4}

    if center is not None:
        navigation["post"]["position"]["voxelCoordinates"] = center

    payload = OrderedDict(
        [
            ("layers", layers),
            ("navigation", navigation),
            ("showSlices", False),
            ("layout", "xy-3d")
        ]
    )
    return payload


def generate_link(param):
    ng_host = param.get("NG_HOST", "state-share-dot-neuroglancer-dot-seung-lab.appspot.com")
    payload = generate_ng_payload(param)

    url = "<https://{host}/#!{payload}|*neuroglancer link*>".format(
        host=ng_host,
        payload=urllib.parse.quote(json.dumps(payload)))

    return url


def dataset_resolution(path, mip=0):
    from cloudvolume import CloudVolume
    vol = CloudVolume(path, mip=mip)
    return vol.resolution.tolist()
