Web Service Load Tester
-



### Usage
```python
import json
from barazmoon import BarAzmoon



class MyLoadTester(BarAzmoon):
    endpoint="http://IP:PORT/PATH"
    http_method="post"
    timeout = 2
    
    def get_workload(self):
        return [7, 12, 0, 31, ...]  # each item of the list is the number of request for a second
        ...
    
    @classmethod
    def get_request_data(cls) -> str:
        return json.dumps({"KEY": "VALUE"})

    @classmethod
    def process_response(cls, response: json):
        value = response.get("KEY")
        do_sth(value)

tester = MyLoadTester(...)
tester.start()
```
