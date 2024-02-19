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
    name: str
    cloudpath: str
    type: LayerType


class ImageLayer(Layer):

    def __init__(self, name: str, cloudpath: str):
        self.name = name
        self.cloudpath = cloudpath
        self.type = LayerType.Image


class SegLayer(Layer):

    def __init__(self, name: str, cloudpath: str):
        self.name = name
        self.cloudpath = cloudpath
        self.type = LayerType.Segmentation


def generate_ng_payload(
    layers: list[Layer], center: Optional[tuple[int, int, int]] = None
) -> dict:
    """Makes neuroglancer link payloads from mappings between layer names and paths.

    Sets the viewer resolution equal to the first layer's first resolution.
    """
    assert len(layers) > 0, "empty layer paths"

    # uses the first resolution and center (through its bounds)
    base_resolution, base_bounds = layer_resolution_and_bounds(layers[0])

    ngl_layers = OrderedDict()
    for layer in layers:
        ngl_layers[layer.name] = {
            "source": default_to_precomputed_path(layer),
            "type": layer.type.value,
        }

    navigation = {"pose": {"position": {"voxelSize": base_resolution}}, "zoomFactor": 4}

    if center is not None:
        navigation["pose"]["position"]["voxelCoordinates"] = center
    else:
        # default to the center of the default_bounds
        navigation["pose"]["position"]["voxelCoordinates"] = tuple(
            map(int, (base_bounds.minpt + base_bounds.maxpt) // 2)
        )

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
    host: str = "spelunker.cave-explorer.org",
) -> str:
    payload = generate_ng_payload(layers)

    return wrap_payload(payload, host)


def wrap_payload(
    payload: Union[OrderedDict, str],
    host: str = "spelunker.cave-explorer.org",
    link_text: str = "neuroglancer link"
) -> str:
    if isinstance(payload, OrderedDict):
        payload = urllib.parse.quote(json.dumps(payload))

    return f"<https://{host}/#!{payload}|*{link_text}*>"


def default_to_precomputed_path(layer: Layer):
    return (
        layer.cloudpath
        if (
            layer.cloudpath.startswith("precomputed://")
            or layer.cloudpath.startswith("graphene://")
        )
        else f"precomputed://{layer.cloudpath}"
    )


def layer_resolution_and_bounds(layer_or_path: Union[str, Layer], mip=0):
    from cloudvolume import CloudVolume
    path = (
        layer_or_path.cloudpath if isinstance(layer_or_path, Layer) else layer_or_path
    )
    vol = CloudVolume(path, mip=mip)

    return vol.resolution.tolist(), vol.bounds
