import time
from typing import List, Tuple
import numpy as np
from multiprocessing import Process, Value, active_children
import asyncio
from aiohttp import ClientSession
from seldon_core.seldon_client import SeldonClient


class BarAzmoon:
    def __init__(self, endpoint, http_method):
        self.endpoint: str = endpoint
        # timeout: int = None  # timeout: seconds to wait for server to respond to request, ignore when timed out
        self.http_method = http_method
        self._workload = self.get_workload()
        self._success_counter = Value("i", 0)  # Actually, those who did not timed out
        self._counter = 0
    
    def get_workload(self) -> List[int]:
        raise NotImplementedError
    
    def start(self):
        total_seconds = 0
        for rate in self._workload:
            total_seconds += 1
            self._counter += rate
            generator_process = Process(
                target=self.target_process, args=(
                    rate, self._success_counter))
            generator_process.daemon = True
            generator_process.start()
            active_children()
            time.sleep(1)
        print("Spawned all the processes. Waiting to finish...")
        for p in active_children():
            p.join()
        
        print(f"total seconds: {total_seconds}")

        return (self._counter, self._counter - self._success_counter.value)

    def target_process(self, count, success_counter):
        asyncio.run(self.generate_load_for_second(count, success_counter))

    async def generate_load_for_second(self, count, success_counter):
        async with ClientSession() as session:
            delays = np.cumsum(
                np.random.exponential(1 / (count * 1.5), count))
            tasks = []
            for i in range(count):
                task = asyncio.ensure_future(
                    self.predict(delays[i], session, success_counter))
                tasks.append(task)
            await asyncio.gather(*tasks)
    
    async def predict(self, delay, session, success_counter):
        await asyncio.sleep(delay)
        data_id, data = self.get_request_data()
        async with getattr(
            session, self.http_method)(self.endpoint, data=data) as response:
            response = await response.json()
            if "error" not in response.keys():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, self.increment_value, success_counter)
            self.process_response(data_id, response)
            return 1
    
    def increment_value(self, value):
        value.value += 1

    def get_request_data(self) -> Tuple[str, str]:
        return None, None
    
    def process_response(self, data_id: str, response: dict):
        pass

class SeldonBarAzmoon(BarAzmoon):
    def set_seldon_params(
        self, gateway_endpoint, deployment_name, namespace):
        self.sc = SeldonClient(
            gateway_endpoint=gateway_endpoint,
            gateway="istio",
            transport="rest",
            deployment_name=deployment_name,
            namespace=namespace)

    async def predict(self, delay, session, success_counter):
        await asyncio.sleep(delay)
        data_id, data = self.get_request_data()
        async with self.sc.predict(inputs = data) as response:
            if response.success:
                json_data_timer = response.response['jsonData']['time']
                return 1

class MLServerBarAzmoon(BarAzmoon):
    def __init__(self, endpoint, http_method, workload):
        self._workload = workload
        super().__init__(endpoint, http_method)

    def get_workload(self) -> List[int]:
        return self._workload

    async def predict(self, delay, session, success_counter):
        await asyncio.sleep(delay)
        data = self.get_request_data()

        async with getattr(
            session, self.http_method)(
                self.endpoint, json=data) as response:
            response = await response.json()
            print(response)
            if "error" not in response.keys():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, self.increment_value, success_counter)
            return 1
    
    def get_request_data(self) -> Tuple[str, str]:
        input_ins = {
            "name": "parameters-np",
            "datatype": "FP32",
            "shape": [2, 1],
            "data": [1, 2],
            "parameters": {
                "content_type": "np"
                }
            }
        payload = {
            "inputs": [input_ins]
        }
        return payload
