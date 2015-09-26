[Index](./index.md) > Getting Started
***

# Getting Started

The first step is to create a python module which hosts the task queue application.

In this example, we create a single file application which should give you a good indication on how to expand to modular backend.
```python
import os
import pq

this_file = os.path.basename(__file__)

task_paths = [this_file]

class EchoJob(pq.Job):

    def __call__(self, echo=None):
    	return echo

def app():
    # Create the task application
    return pq.TaskApp(config=this_file)

if __name__ == '__main__':
    app().start()
```
