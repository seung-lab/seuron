"""Neuroglancer link functions."""
import json
import urllib
from typing import Optional, Union
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum


class LayerType(Enum):
    Image = "image"
    Segmentation = "segmentation"


@dataclass
class Layer:
    cloudpath: str
    type: LayerType


class ImageLayer(Layer):

    def __init__(self, cloudpath: str):
        self.cloudpath = cloudpath
        self.type = LayerType.Image


class SegLayer(Layer):

    def __init__(self, cloudpath: str):
        self.cloudpath = cloudpath
        self.type = LayerType.Segmentation


def generate_ng_payload(
    layers: dict[str, Layer], center: Optional[tuple[int, int, int]] = None
) -> str:
    """Makes neuroglancer link payloads from mappings between layer names and paths.

    Makes the viewer resolution equal to the "img" layer if it exists in layers.
    Otherwise, it takes the first it can find.
    """
    assert len(layers) > 0, "empty layer paths"

    ngl_layers = OrderedDict()
    layers = layers.copy()  # for in-place modifications later

    # default to first resolution we can find - update if we find an image
    ng_resolution = dataset_resolution(list(layers.values())[0])

    # Trying to find an image layer
    base_img = layers.pop("img", None)
    if base_img:
        ngl_layers["img"] = {
            "source": fullpath(base_img),
            "type": base_img.type.value,
        }
        ng_resolution = dataset_resolution(base_img)

    for key, layer in layers.items():
        ngl_layers[key] = {
            "source": fullpath(layer),
            "type": layer.type.value,
        }

    navigation = {"pose": {"position": {"voxelSize": ng_resolution}}, "zoomFactor": 4}

    if center is not None:
        navigation["post"]["position"]["voxelCoordinates"] = center

    payload = OrderedDict(
        [
            ("layers", ngl_layers),
            ("navigation", navigation),
            ("showSlices", False),
            ("layout", "xy-3d")
        ]
    )
    return payload


def generate_link(
    layers: dict[str, Layer],
    host: str = "state-share-dot-neuroglancer-dot-seung-lab.appspot.com",
) -> str:
    payload = generate_ng_payload(layers)

    return wrap_payload(payload, host)


def wrap_payload(
    payload: Union[OrderedDict, str],
    host: str = "state-share-dot-neuroglancer-dot-seung-lab.appspot.com",
    link_text: str = "neuroglancer link"
) -> str:
    if isinstance(payload, OrderedDict):
        payload = urllib.parse.quote(json.dumps(payload))

    return f"<https://{host}/#!{payload}|*{link_text}*>"


def fullpath(layer: Layer):
    return (
        layer.cloudpath
        if layer.cloudpath.startswith("precomputed://")
        else f"precomputed://{layer.cloudpath}"
    )


def dataset_resolution(layer_or_path: Union[str, Layer], mip=0) -> tuple:
    from cloudvolume import CloudVolume
    path = (
        layer_or_path.cloudpath if isinstance(layer_or_path, Layer) else layer_or_path
    )
    vol = CloudVolume(path, mip=mip)
    return vol.resolution.tolist()
