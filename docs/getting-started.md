
# Getting Started

The first step is to create a python module which hosts the task queue application.
Lets assume this module is called ``main.py``:
```python
from pq import PulsarQueue

app = PulsarQueue()

if __name__ == '__main__':
	app.start()
```
