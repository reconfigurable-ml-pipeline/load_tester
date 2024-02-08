from typing import List, Tuple, Any
import base64
import numpy as np

import mlserver.grpc.converters as converters
import aiohttp.payload as aiohttp_payload
import mlserver.types as types

from .main import BarAzmoonAsyncRest
from .main import BarAzmoonAsyncGrpc
from .main import Data


def encode_to_bin(im_arr):
    im_bytes = im_arr.tobytes()
    im_base64 = base64.b64encode(im_bytes)
    input_dict = im_base64.decode()
    return input_dict


class MLServerAsyncRest:
    def __init__(
        self,
        *,
        workload: List[int],
        endpoint: str,
        data: Any,
        data_shape: List[int],
        mode: str = "step",  # options - step, equal, exponential
        benchmark_duration: int = 1,
        data_type: str,
        http_method="post",
        **kwargs,
    ):
        self.endpoint = endpoint
        self.http_method = http_method
        self._workload = (rate for rate in workload)
        self._counter = 0
        self.data_type = data_type
        self.data = data
        self.data_shape = data_shape
        self.mode = mode
        self.kwargs = kwargs
        self.benchmark_duration = benchmark_duration
        _, self.payload = self.get_request_data()

    async def start(self):
        c = BarAzmoonAsyncRest(
            self.endpoint, self.payload, self.mode, self.benchmark_duration
        )
        await c.benchmark(self._workload)
        await c.close()
        return c.responses

    def get_request_data(self) -> Tuple[str, str]:
        if self.data_type == "example":
            payload = {
                "inputs": [
                    {
                        "name": "parameters-np",
                        "datatype": "FP32",
                        "shape": self.data_shape,
                        "data": self.data,
                        "parameters": {"content_type": "np"},
                    }
                ]
            }
        elif self.data_type == "audio":
            payload = {
                "inputs": [
                    {
                        "name": "array_inputs",
                        "shape": self.data_shape,
                        "datatype": "FP32",
                        "data": self.data,
                        "parameters": {"content_type": "np"},
                    }
                ]
            }
        elif self.data_type == "audio-base64":
            payload = {
                "inputs": [
                    {
                        "name": "parameters-np",
                        "datatype": "BYTES",
                        "shape": self.data_shape,
                        "data": encode_to_bin(np.array(self.data, dtype=np.float32)),
                        "parameters": {"content_type": "np", "dtype": "f4"},
                    }
                ]
            }
        elif self.data_type == "text":
            payload = {
                "inputs": [
                    {
                        "name": "text_inputs",
                        "shape": self.data_shape,
                        "datatype": "BYTES",
                        "data": [self.data],
                        "parameters": {"content_type": "str"},
                    }
                ]
            }
        elif self.data_type == "image":
            payload = {
                "inputs": [
                    {
                        "name": "parameters-np",
                        "datatype": "INT32",
                        "shape": self.data_shape,
                        "data": self.data,
                        "parameters": {"content_type": "np"},
                    }
                ]
            }
        elif self.data_type == "image-base64":
            payload = {
                "inputs": [
                    {
                        "name": "parameters-np",
                        "datatype": "BYTES",
                        "shape": self.data_shape,
                        "data": encode_to_bin(np.array(self.data)),
                        "parameters": {"content_type": "np", "dtype": "u1"},
                    }
                ]
            }
        else:
            raise ValueError(f"Unkown datatype {self.kwargs['data_type']}")
        return None, aiohttp_payload.JsonPayload(payload)


class MLServerAsyncGrpc:
    def __init__(
        self,
        *,
        workload: List[int],
        endpoint: str,
        data: List[Data],
        model: str,
        data_type: str,
        metadata: List[Tuple[str, str]],
        mode,  # options - step, equal, exponential
        client_batch: int = 1,
        benchmark_duration: int = 1,
        ignore_output: bool = False,
        grpc_max_message_length: int = 104857600,
        sla: int = 0,
        **kwargs,
    ):
        self.endpoint = endpoint
        self.metadata = metadata
        self.model = model
        self._workload = (rate for rate in workload)
        self._counter = 0
        self.data_type = data_type
        self.data = data
        self.kwargs = kwargs
        self.mode = mode
        self.benchmark_duration = benchmark_duration
        self.client_batch = client_batch
        self.ignore_output = ignore_output
        self.grpc_max_message_length = grpc_max_message_length
        self.sla = sla
        self.payloads = self.get_request_data()

    async def start(self):
        c = BarAzmoonAsyncGrpc(
            self.endpoint,
            self.metadata,
            self.payloads,
            self.mode,
            self.benchmark_duration,
            self.ignore_output,
            # self.grpc_max_message_length
        )
        await c.benchmark(self._workload)
        return c.responses
    
    # def stop(self):
    #     self.c.stop()
        

    def get_request_data(self) -> Tuple[str, str]:
        payloads = []
        for data_ins in self.data:
            # if self.data_type == 'audio':
            #     payload = types.InferenceRequest(
            #         inputs=[
            #             types.RequestInput(
            #                 name="audio",
            #                 shape=data_ins.data_shape,
            #                 datatype="FP32",
            #                 data=self.data,
            #                 parameters=types.Parameters(
            #                     content_type="np",
            #                     custom_parameters=data_ins.parameters
            #                     ),
            #                 )
            #             ]
            #         )
            if self.data_type == "text":
                payload = types.InferenceRequest(
                    inputs=[
                        types.RequestInput(
                            name="inputs",
                            shape=[1],
                            datatype="BYTES",
                            data=[data_ins.data.encode("utf8")],
                            parameters=types.Parameters(
                                content_type="str", **data_ins.custom_parameters
                            ),
                        )
                    ]
                )
            # elif self.data_type == 'image':
            #     payload =  types.InferenceRequest(
            #         inputs=[
            #             types.RequestInput(
            #             name="image",
            #             shape=data_ins.data_shape,
            #             datatype="INT32",
            #             data=data_ins.data,
            #             parameters=types.Parameters(
            #                 content_type="np",
            #                 **data_ins.custom_parameters),
            #             )
            #         ]
            #     )
            elif self.data_type == "image":
                # extended_parameters = [{
                #     "node_name": [1], "arrival": [2], "serving": [3],
                #     "dtype": ['u1'], "datashape": [[2,2]]}]
                extended_parameters = {
                    "dtype": "u1",
                    "datashape": data_ins.data_shape,
                    "node_name": data_ins.node_name,
                    "arrival": data_ins.arrival,
                    "serving": data_ins.serving,
                    "next_node": data_ins.next_node,
                    'sla': self.sla}
                # extended_parameters_repeated = [{
                #     "dtype": "u1",
                #     "datashape": data_ins.data_shape,
                #     "node_name": data_ins.node_name,
                #     "arrival": data_ins.arrival,
                #     "serving": data_ins.serving,
                #     "next_node": data_ins.next_node,
                #      "sla": data_ins.sla},
                #     {
                #     "dtype": "u1",
                #     "datashape": data_ins.data_shape,
                #     "node_name": data_ins.node_name,
                #     "arrival": data_ins.arrival,
                #     "serving": data_ins.serving,
                #     "next_node": data_ins.next_node,
                #     "sla": data_ins.sla}
                #     ]
                payload = types.InferenceRequest(
                    inputs=[
                        types.RequestInput(
                            name="image-bytes",
                            shape=[1],
                            # shape=[2],
                            datatype="BYTES",
                            # data=[data_ins.data.tobytes()] * self.client_batch * 2,
                            data=[data_ins.data.tobytes()] * self.client_batch,
                            parameters=types.Parameters(
                                # dtype="u1",
                                # datashape=str(data_ins.data_shape),
                                # extended_parameters_repeated=extended_parameters_repeated,
                                extended_parameters=extended_parameters,
                                # **data_ins.custom_parameters,
                            ),
                        )
                    ]
                )
            elif self.data_type == "audio":
                payload = types.InferenceRequest(
                    inputs=[
                        types.RequestInput(
                            name="audio-bytes",
                            shape=[1],
                            datatype="BYTES",
                            data=[data_ins.data.astype(np.float32).tobytes()],
                            parameters=types.Parameters(
                                dtype="f4",
                                datashape=str(data_ins.data_shape),
                                **data_ins.custom_parameters,
                            ),
                        )
                    ]
                )
            else:
                raise ValueError(f"Unkown datatype {self.kwargs['data_type']}")
            payload = converters.ModelInferRequestConverter.from_types(
                payload, model_name=self.model, model_version=None
            )
            payloads.append(payload)
        return payloads
