import time
from typing import List, Tuple
import numpy as np
from multiprocessing import Process, Value, active_children
import asyncio
from aiohttp import ClientSession


class BarAzmoon:
    endpoint: str = None
    # timeout: int = None  # timeout: seconds to wait for server to respond to request, ignore when timed out
    http_method = "get"
    def __init__(self):
        self.__workload = self.get_workload()
        self.__success_counter = Value("i", 0)  # Actually, those who did not timed out
        self.__counter = 0
    
    def get_workload(self) -> List[int]:
        raise NotImplementedError
    
    def start(self):
        total_seconds = 0
        for rate in self.__workload:
            total_seconds += 1
            self.__counter += rate
            generator_process = Process(target=self.target_process, args=(rate, self.__success_counter))
            generator_process.daemon = True
            generator_process.start()
            active_children()
            time.sleep(1)
        print("Spawned all the processes. Waiting to finish...")
        for p in active_children():
            p.join()
        
        print(f"total seconds: {total_seconds}")

        return (self.__counter, self.__counter - self.__success_counter.value)

    @classmethod
    def target_process(cls, count, success_counter):
        asyncio.run(cls.generate_load_for_second(count, success_counter))

    @classmethod
    async def generate_load_for_second(cls, count, success_counter):
        async with ClientSession() as session:
            delays = np.cumsum(np.random.exponential(1 / (count * 1.5), count))
            tasks = []
            for i in range(count):
                task = asyncio.ensure_future(cls.predict(delays[i], session, success_counter))
                tasks.append(task)
            await asyncio.gather(*tasks)
    
    @classmethod
    async def predict(cls, delay, session, success_counter):
        await asyncio.sleep(delay)
        data_id, data = cls.get_request_data()
        async with getattr(session, cls.http_method)(cls.endpoint, data=data) as response:
            response = await response.json()
            if "error" not in response.keys():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, cls.increment_value, success_counter)
            cls.process_response(data_id, response)
            return 1
    
    @staticmethod
    def increment_value(value):
        value.value += 1

    @classmethod
    def get_request_data(cls) -> Tuple[str, str]:
        return None, None
    
    @classmethod
    def process_response(cls, data_id: str, response: dict):
        pass
