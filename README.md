# Web Service Load Tester

> REST and json-based load tester implemented using asyncio and multiprocessing

### Usage
---

```python
import json
from barazmoon import BarAzmoon


class MyLoadTester(BarAzmoon):
    def get_request_data(self) -> Tuple[str, str]:
        return sending_data_id, sending_data

    def process_response(self, sent_data_id: str, response: json):
        do_sth(response, sent_data_id)


workload = [7, 12, 0, 31, ...]  # each item of the list is the number of request for a second
tester = MyLoadTester(workload=workload, endpoint="http://IP:PORT/PATH", http_method="post")
tester.start()
```
