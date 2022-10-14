import time
from typing import List, Tuple
import numpy as np
from multiprocessing import Process, active_children
import asyncio
from aiohttp import ClientSession
import aiohttp.payload as aiohttp_payload

class BarAzmoon:
    def __init__(self, *, workload: List[int], endpoint: str, http_method = "get", **kwargs):
        self.endpoint = endpoint
        self.http_method = http_method
        self.__workload = (rate for rate in workload)
        self.__counter = 0
        self.kwargs = kwargs
    
    def start(self):
        total_seconds = 0
        for rate in self.__workload:
            total_seconds += 1
            self.__counter += rate
            generator_process = Process(target=self.target_process, args=(rate,))
            generator_process.daemon = True
            generator_process.start()
            active_children()
            time.sleep(1)
        print("Spawned all the processes. Waiting to finish...")
        for p in active_children():
            p.join()
        
        print(f"total seconds: {total_seconds}")

        return self.__counter, total_seconds

    def target_process(self, count):
        asyncio.run(self.generate_load_for_second(count))

    async def generate_load_for_second(self, count):
        async with ClientSession() as session:
            delays = np.cumsum(np.random.exponential(1 / (count * 1.5), count))
            tasks = []
            for i in range(count):
                task = asyncio.ensure_future(self.predict(delays[i], session))
                tasks.append(task)
            await asyncio.gather(*tasks)
    
    async def predict(self, delay, session):
        await asyncio.sleep(delay)
        data_id, data = self.get_request_data()
        async with getattr(session, self.http_method)(self.endpoint, data=data) as response:
            response = await response.json()
            self.process_response(data_id, response)
            return 1
    
    def get_request_data(self) -> Tuple[str, str]:
        return None, None
    
    def process_response(self, data_id: str, response: dict):
        pass


class MLServerBarAzmoon(BarAzmoon):
    def get_request_data(self) -> Tuple[str, str]:
        if self.kwargs['data_type'] == 'example':
            payload = {
                "inputs": [
                    {
                        "name": "parameters-np",
                        "datatype": "FP32",
                        "shape": self.kwargs['data_shape'],
                        "data": self.kwargs['data'],
                        "parameters": {
                            "content_type": "np"
                        }
                    }]
                }
        elif self.kwargs['data_type'] == 'audio':
            payload = {
                "inputs": [
                    {
                    "name": "array_inputs",
                    "shape": self.kwargs['data_shape'],
                    "datatype": "FP32",
                    "data": self.kwargs['data'],
                    "parameters": {
                        "content_type": "np"
                    }
                    }
                ]
            }
        elif self.kwargs['data_type'] == 'text':
            payload = {
                "inputs": [
                    {
                        "name": "text_inputs",
                        "shape": [1],
                        "datatype": "BYTES",
                        "data": self.kwargs['data'],
                        "parameters": {
                            "content_type": "str"
                        }
                    }
                ]
            }
        elif self.kwargs['data_type'] == 'image':
            payload = {
                "inputs":[
                    {
                        "name": "parameters-np",
                        "datatype": "INT32",
                        "shape": self.kwargs['data_shape'],
                        "data": self.kwargs['data'],
                        "parameters": {
                            "content_type": "np"
                            }
                    }]
                }
        else:
            raise ValueError(f"Unkown datatype {self.kwargs['data_type']}")
        return None, aiohttp_payload.JsonPayload(payload)

    def process_response(self, data_id: str, response: dict):
        print(response)