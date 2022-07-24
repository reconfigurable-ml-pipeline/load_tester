import time
import json
from typing import List, Tuple
import numpy as np
from multiprocessing import Process, Value
import asyncio
from aiohttp import ClientSession


class BarAzmoon:
    endpoint: str = None
    timeout: int = None  # timeout: seconds to wait for server to respond to request, ignore when timed out
    http_method = "get"
    def __init__(self):
        self.workload = self.get_workload()
        self.counter = Value("i", 0)
        self.success_counter = Value("i", 0)  # Actually, those who did not timed out
    
    def get_workload(self) -> List[int]:
        raise NotImplementedError
    
    def start(self):
        processes = []
        total_seconds = 0
        successful_seconds = 0
        timed_out_seconds = 0
        for rate in self.workload:
            total_seconds += 1
            generator_process = Process(target=self.target_process, args=(rate, self.counter, self.success_counter))
            generator_process.daemon = True
            generator_process.start()
            processes.append(generator_process)
            time.sleep(1)
            procs = []
            if self.timeout:
                for i in range(len(processes) - self.timeout):
                    p: Process = processes[i]
                    if p.exitcode is None:
                        timed_out_seconds += 1
                        p.kill()
                    else:
                        successful_seconds += 1
                        p.join()
            else:
                for p in processes:
                    if p.exitcode is None:
                        procs.append(p)
                    else:
                        successful_seconds += 1
            processes = procs
        
        print("Spawned all the processes. Status of processes till now is:")
        print(f"total seconds: {total_seconds}, successful seconds: {successful_seconds}, timedout seconds: {timed_out_seconds}")
        if self.timeout:
            print(f"Waiting for {self.timeout} seconds...")
            time.sleep(self.timeout)
            for p in processes:
                if p.exitcode is None:
                    timed_out_seconds += 1
                    p.kill()
                else:
                    successful_seconds += 1
                    p.join()
        else:
            print("Waiting for processes to finish...")
            for p in processes:
                p.join()

        print("Final status of processes is:")
        print(
            f"total seconds: {total_seconds}, successful seconds: {successful_seconds}, timedout seconds: {timed_out_seconds}"
        )

        return (self.counter.value, self.counter.value - self.success_counter.value)

    @classmethod
    def target_process(cls, count, counter, success_counter):
        asyncio.run(cls.generate_load_for_second(count, counter, success_counter))

    @classmethod
    async def generate_load_for_second(cls, count, counter, success_counter):
        async with ClientSession() as session:
            delays = np.cumsum(np.random.exponential(1 / count, count) / 2)
            tasks = []
            for i in range(count):
                task = asyncio.ensure_future(cls.predict(delays[i], session, success_counter))
                tasks.append(task)
                counter.value += 1
            await asyncio.gather(*tasks)
    
    @classmethod
    async def predict(cls, delay, session, success_counter):
        await asyncio.sleep(delay)
        data_id, data = cls.get_request_data()
        async with getattr(session, cls.http_method)(cls.endpoint, data=data) as response:
            response = await response.text()
            response = json.loads(response)
            success_counter.value += 1
            cls.process_response(data_id, response)
            return 1

    @classmethod
    def get_request_data(cls) -> Tuple[str, str]:
        return None, None
    
    @classmethod
    def process_response(cls, data_id: str, response: dict):
        pass
