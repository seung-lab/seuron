import os
import json
import onnx
from datetime import datetime
from typing import List
from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator, computed_field

from cloudfiles import dl
from cloudvolume import CloudVolume


class RawSegParams(BaseModel):
    input_path: str | None = Field(default=None, description="""Input data for segmentation, valid CloudVolume URL: string should start with "gs://", "s3://", "tigerdata://", or starts with "https://". No trailing "/" or whitespaces, the url should look like a folder, does not end with an ext like .txt or .zip or .onnx. Prefer the url user explicitly specified as input data""")
    output_path: str | None = Field(default=None, description="""Output location for segmentation, valid CloudVolume URL: string should start with "gs://", "s3://", "tigerdata://", or starts with "https://". No trailing "/" or whitespaces, the url should look like a folder, does not end with an ext like .txt or .zip or .onnx. Prefer the url user explicitly specified as output location, Cannot be the same as input_path""")
    input_resolution: List[int | float] | None = Field(default=None, min_items=3, max_items=3, description="""Resolution of the input data, the 3 numbers may be specified by user as [x_voxel_size, y_voxel_size, z_voxel_size] or x_voxel_size x y_voxel_size x z_voxel_size, with or without physical units. The input_resolution must be contained in the available_resolutions if it is not null, but do not pick resolutions in available_resolutions without hints from the user input. If input_resolution is not in the list of available_resolutions, leave it to None""")
    input_bbox: List[int] | None = Field(default=None, min_items=6, max_items=6, description="""Bounding box for segmentation, the input bounding box is a list of 6 numbers in the form of  [minpt_x, minpt_y, minpt_z, maxpt_x, maxpt_y, maxpt_z], the user can also specify the starting or center point [pt_x, pt_y, pt_z] and the size of the bounding box they want. Do not use dataset_bounds unless user explicitly want to segment the entire dataset""")
    inference_model: str | None = Field(default=None, description="""Affinity model for segmentation, valid CloudFiles URL: any string starting with "gs://", "s3://", "tigerdata://", or starts with "https://". Remove any trailing "/" or whitespaces. The url should end with ext .onnx""")


class InferredParams(BaseModel):
    input_path: str | None = Field(default=None)
    input_resolution: List[int | float] | None = Field(default=None)

    @computed_field
    @property
    def available_resolutions(self) -> List[List[int | float]] | None:
        if self.input_path is None:
            return None
        try:
            vol = CloudVolume(self.input_path)
            scales = vol.info["scales"]
            return [s["resolution"] for s in scales]
        except:
            return None

    @computed_field
    @property
    def dataset_bounds(self) -> List[int] | None:
        if self.input_path is None or self.input_resolution is None:
            return None
        try:
            vol = CloudVolume(self.input_path, mip=self.input_resolution)
            return [int(x) for x in vol.bounds.to_list()]
        except:
            return None

    def computed_only(self) -> dict:
        computed = self.__class__.__pydantic_computed_fields__
        return {name: getattr(self, name) for name in computed}


class ValidatedSegParams(RawSegParams):
    __onnx_param: dict = PrivateAttr()

    @field_validator('input_path', mode='after')
    def check_input_path(cls, value):
        if value is None:
            raise ValueError("Missing input path")
        try:
            vol = CloudVolume(value)
        except:
            raise ValueError("Cannot open the input path")

        if vol.info["type"] != "image":
            raise ValueError("The given input is not an image stack")

        return value

    @field_validator('output_path', mode='after')
    def check_output_path(cls, value):
        if value is None:
            raise ValueError("Missing output path")

        return value

    @field_validator('input_resolution', mode='after')
    def check_input_resolution(cls, value):
        if value is None:
            raise ValueError("Missing input resolution")

        return value

    @field_validator('input_bbox', mode='after')
    def check_input_bbox(cls, value):
        if value is None:
            raise ValueError("Missing input bounding box")
        if any(value[i+3] < value[i] for i in range(3)):
            raise ValueError("Bounding box error: max pt > min pt")

        return value

    @field_validator('inference_model', mode='after')
    def check_inference_model(cls, value):
        if value is None:
            raise ValueError("Missing inference model")

        return value

    @model_validator(mode='after')
    def validate_parameters(self):
        try:
            vol = CloudVolume(self.input_path, mip=self.input_resolution)
        except Exception:
            raise ValueError("Cannot open the input path at specific resolution")

        if not vol.image.has_data(self.input_resolution):
            raise ValueError("No data in the specified resolution")

        if self.output_path == self.input_path:
            raise ValueError("Output path cannot be the same as the input path")

        available_resolutions = [s["resolution"] for s in vol.info["scales"]]
        if self.input_resolution is None:
            raise ValueError(f"Pick a resolution from {json.dumps(available_resolutions)}")
        elif self.input_resolution not in available_resolutions:
            raise ValueError(f"The specified resolution does not exist, pick a resolution from {json.dumps(available_resolutions)}")

        dataset_bounds = [int(x) for x in vol.bounds.to_list()]

        if any(x <= y for x, y in zip(self.input_bbox[:3], dataset_bounds[:3])) or any(x > y for x, y in zip(self.input_bbox[3:], dataset_bounds[3:])):
            raise ValueError(f"bounding box must be contained within the dataset bounds: {json.dump(dataset_bounds)}")

        dim = [self.input_bbox[i+3] - self.input_bbox[i] for i in range(3)]

        if any(x > 10000 for x in dim):
            raise ValueError("bounding box too large, make sure the bounding box has less than 10K voxel in each direction ")

        try:
            onnx_file = dl(self.inference_model)
            onnx_model = onnx.load_model_from_string(onnx_file["content"])
        except:
            raise ValueError("Invalid inference model")

        inputs = onnx_model.graph.input
        outputs = onnx_model.graph.output
        output_dim = [dim.dim_value for dim in outputs[0].type.tensor_type.shape.dim]
        input_shape = [dim.dim_value for dim in inputs[0].type.tensor_type.shape.dim][-3:][::-1]
        if all(x == output_dim[-1] for x in output_dim[-3:]):
            crop_fraction = [0.75, 0.75, 0.75]
        else:
            crop_fraction = [0.75, 0.75, 0.8]
        output_shape = [int(x*y) for x, y in zip(input_shape, crop_fraction)]
        self.__onnx_param = {
            "input_shape": input_shape,
            "output_shape": output_shape,
            "output_dim": output_dim,
        }
        return self

    def execute(self, context):
        from kombu_helper import put_message
        from airflow_api import set_variable, run_dag
        from bot_info import broker_url
        from bot_utils import create_run_token
        from pipeline_commands import handle_batch, supply_default_param

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        aff_path = os.path.join(self.output_path, timestamp, "inference")
        inference_output_channel = self.__onnx_param["output_dim"][1]
        if inference_output_channel == 1:
            output_channel = 1
        else:
            output_channel = 3
        inf_params = {
            "IMAGE_PATH": self.input_path,
            "IMAGE_FILL_MISSING": True,
            "IMAGE_RESOLUTION": self.input_resolution,
            "OUTPUT_PATH": aff_path,
            "BBOX": self.input_bbox,
            "INPUT_PATCH_SIZE": self.__onnx_param["input_shape"],
            "OUTPUT_PATCH_SIZE": self.__onnx_param["output_shape"],
            "INFERENCE_OUTPUT_CHANNELS": inference_output_channel,
            "OUTPUT_CHANNELS": output_channel,
            "CHUNKFLOW_IMAGE": "us-docker.pkg.dev/neuromancer-seung-import/ranlu/chunkflow:trt",
            "MAX_RAM": 2,
            "ENABLE_FP16": True,
            "ONNX_MODEL_PATH": self.inference_model,
        }
        ng_prefix = os.path.join(self.output_path, timestamp, "ng") + "/"
        scratch_prefix = os.path.join(self.output_path, timestamp, "scratch") + "/"
        seg_params = {
            "IMAGE_PATH": self.input_path,
            "INHERIT_PARAMETERS": False,
            "AFF_PATH": aff_path,
            "NG_PREFIX": ng_prefix,
            "SCRATCH_PREFIX": scratch_prefix,
            "WS_HIGH_THRESHOLD": "0.999999",
            "WS_LOW_THRESHOLD": "0.000001",
            "WS_SIZE_THRESHOLD": "200",
            "WS_DUST_THRESHOLD": "200",
            "AGG_THRESHOLD": "0.3",
            "WORKER_IMAGE": "ranlu/abiss:main",
            "SKIP_SKELETON": True,
        }
        put_message(broker_url, "seuronbot_payload", [inf_params, seg_params])
        supply_default_param(inf_params)
        set_variable("inference_param", inf_params, serialize_json=True)
        state = run_dag("chunkflow_generator", wait_for_completion=True).state
        if state != "success":
            return f"Failed to run with inference parameter: \n {json.dumps(params[0], indent=4)}"

        create_run_token(context)
        handle_batch("inf_run", context)
