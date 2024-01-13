import time
from typing import List
import numpy as np
import asyncio
from numpy.random import default_rng
import aiohttp
import asyncio
import grpc
from mlserver.codecs.string import StringRequestCodec
from mlserver.codecs.numpy import NumpyRequestCodec
import mlserver.grpc.dataplane_pb2_grpc as dataplane
import mlserver.grpc.converters as converters
from copy import deepcopy


def decode_from_bin(
    output: List[bytes], shape: List[int], dtype: str
) -> List[np.array]:

    buff = memoryview(output)
    array = np.frombuffer(buff, dtype=dtype).reshape(shape)
    return array


class Data:
    def __init__(
        self, data, data_shape, custom_parameters={"custom": "custom"}
    ) -> None:
        self.data = data
        self.data_shape = data_shape
        self.custom_parameters = custom_parameters


# MAX_QUEUE_SIZE = 1000000
# queue = Queue(MAX_QUEUE_SIZE)
# lock = Lock()

# TCP_CONNECTIONS = 500
# connector = TCPConnector(limit=TCP_CONNECTIONS)

TIMEOUT = 20 * 60


# ============= Pure Async Rest based load tester =============


async def request_after_rest(session, url, wait, payload):
    if wait:
        await asyncio.sleep(wait)
    sending_time = time.time()
    try:
        async with session.post(url, data=payload, timeout=TIMEOUT) as resp:
            if resp.status != 200:
                resp = {"failed": await resp.text()}  # TODO: maybe raise!
            else:
                resp = await resp.json()
            arrival_time = time.time()
            timing = {
                "time": {"sending_time": sending_time, "arrival_time": arrival_time}
            }
            resp.update(timing)
            return resp
    except asyncio.exceptions.TimeoutError:
        resp = {"failed": "timeout"}
        arrival_time = time.time()
        timing = {"time": {"sending_time": sending_time, "arrival_time": arrival_time}}
        resp.update(timing)
        return resp


class BarAzmoonAsyncRest:
    def __init__(self, endpoint, payload, mode, benchmark_duration=1):
        """
        endpoint:
            the http path the load testing endpoint
        payload:
            data to the be sent
        """
        self.endpoint = endpoint
        self.payload = payload
        self.session = aiohttp.ClientSession()
        self.responses = []
        self.duration = benchmark_duration
        self.mode = mode

    async def benchmark(self, request_counts):
        tasks = []
        for i, req_count in enumerate(request_counts):
            tasks.append(
                asyncio.ensure_future(
                    self.submit_requests_after(
                        i * self.duration, req_count, self.duration
                    )
                )
            )
        await asyncio.gather(*tasks)

    async def submit_requests_after(self, after, req_count, duration):
        if after:
            await asyncio.sleep(after)
        tasks = []
        beta = duration / req_count
        start = time.time()

        rng = default_rng()
        if self.mode == "step":
            arrival = np.zeros(req_count)
        elif self.mode == "equal":
            arrival = np.arange(req_count) * beta
        elif self.mode == "exponential":
            arrival = rng.exponential(beta, req_count)
        print(
            f"Sending {req_count} requests sent in {time.ctime()} at timestep {after}"
        )
        for i in range(req_count):
            tasks.append(
                asyncio.ensure_future(
                    request_after_rest(
                        self.session,
                        self.endpoint,
                        wait=arrival[i],
                        payload=self.payload,
                    )
                )
            )
        resps = await asyncio.gather(*tasks)

        elapsed = time.time() - start
        if elapsed < duration:
            await asyncio.sleep(duration - elapsed)

        self.responses.append(resps)
        print(
            f"Recieving {len(resps)} requests sent in {time.ctime()} at timestep {after}"
        )

    async def close(self):
        await self.session.close()


# ============= Pure Async Grpc based load tester =============


async def request_after_grpc(stub, metadata, wait, payload, ignore_output=False):
    if wait:
        await asyncio.sleep(wait)
    sending_time = time.time()
    try:
        # extract the infromation based on datatype
        grpc_resp = await stub.ModelInfer(request=payload, metadata=metadata)
        arrival_time = time.time()
        inference_response = converters.ModelInferResponseConverter.to_types(grpc_resp)
        type_of = inference_response.parameters.type_of
        if ignore_output:
            outputs = {"data": "ignore output"}
        else:
            if type_of == "image":
                for request_output in inference_response.outputs:
                    dtype = request_output.parameters.extended_parameters['dtype']
                    shape = request_output.parameters.extended_parameters['datashape']
                    output_data = request_output.data.__root__
                    X = decode_from_bin(output=output_data[0], shape=shape, dtype=dtype)
                    outputs = {"data": X}
            elif type_of == "text":
                raw_json = StringRequestCodec.decode_response(inference_response)
                outputs = {"data": raw_json}
            elif type_of == "int":
                numpy_output = NumpyRequestCodec.decode_response(inference_response)
                outputs = {"data": numpy_output}

        # extract timestamps
        times = {}
        times["request"] = {"sending": sending_time, "arrival": arrival_time}
        resp = {}
        resp["times"] = times
        resp["model_name"] = grpc_resp.model_name
        if hasattr(inference_response.outputs[0].parameters, 'extended_parameters'): # handling drops
            extended_parameters = inference_response.outputs[0].parameters.extended_parameters
            model_times = {}
            for i, node_name in enumerate(extended_parameters['node_name']):
                node = {}
                node["arrival"] = extended_parameters['arrival'][i]
                node["serving"] = extended_parameters['serving'][i]
                model_times[node_name] = deepcopy(node)
            times["models"] = model_times
            resp["outputs"] = [outputs]
        else:
            drop_message = NumpyRequestCodec.decode_response(inference_response)[0]
            resp = {"failed": drop_message}
        return resp
    except asyncio.exceptions.TimeoutError:
        resp = {"failed": "timeout"}
        times = {}
        times["request"] = {"sending": sending_time, "arrival": arrival_time}
        resp["times"] = times
        return resp
    except grpc.RpcError as e:
        resp = {"failed": str(e)}
        times = {}
        try:
            times["request"] = {"sending": sending_time, "arrival": arrival_time}
        except UnboundLocalError:
            times["request"] = {"sending": sending_time}
        resp["times"] = times
        return resp


class BarAzmoonAsyncGrpc:
    def __init__(
        self,
        endpoint: str,
        metadata: str,
        payloads: List[Data],
        mode: str,
        benchmark_duration=1,
        ignore_output=False,
        grpc_max_message_length: int = 104857600
    ):
        """
        endpoint:
            the path the load testing endpoint
        payload:
            data to the be sent
        """
        self.endpoint = endpoint
        self.payloads = payloads
        self.metadata = metadata
        self.responses = []
        self.mode = mode
        self.duration = benchmark_duration
        self.request_index = 0
        self.stop_flag = False
        self.ignore_output = ignore_output
        self.grpc_max_message_length = grpc_max_message_length

    async def benchmark(self, request_counts):
        options = [
            ('grpc.max_send_message_length', self.grpc_max_message_length),
            ('grpc.max_receive_message_length', self.grpc_max_message_length)]
        async with grpc.aio.insecure_channel(self.endpoint, options=options) as ch:
            self.stub = dataplane.GRPCInferenceServiceStub(ch)
            tasks = []
            for i, req_count in enumerate(request_counts):
                tasks.append(
                    asyncio.ensure_future(
                        self.submit_requests_after(
                            i * self.duration, req_count, self.duration
                        )
                    )
                )
            await asyncio.gather(*tasks)

    async def submit_requests_after(self, after, req_count, duration):
        if after:
            await asyncio.sleep(after)
        tasks = []
        beta = duration / req_count
        start = time.time()

        rng = default_rng()
        if self.mode == "step":
            arrival = np.zeros(req_count)
        elif self.mode == "equal":
            arrival = np.arange(req_count) * beta
        elif self.mode == "exponential":
            arrival = rng.exponential(beta, req_count)
        print(
            f"Sending {req_count} requests sent in {time.ctime()} at timestep {after}"
        )
        for i in range(req_count):
            if self.request_index == len(self.payloads):
                self.request_index = 0
            if self.stop_flag:
                break
            tasks.append(
                asyncio.ensure_future(
                    request_after_grpc(
                        self.stub,
                        self.metadata,
                        wait=arrival[i],
                        payload=self.payloads[self.request_index],
                        ignore_output=self.ignore_output
                    )
                )
            )
            self.request_index += 1

        resps = await asyncio.gather(*tasks)

        elapsed = time.time() - start
        if elapsed < duration:
            await asyncio.sleep(duration - elapsed)

        self.responses.append(resps)
        total = len(resps)
        failed = 0
        for resp in resps:
            if "failed" in resp.keys():
                failed += 1
        success = total - failed
        print(
            f"Recieving {total} requests sent in {time.ctime()} at timestep {after}, success rate: {success}/{total}"
        )
        return resps
