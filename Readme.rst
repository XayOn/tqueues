Tqueues
=======

Tqueues (reThinkdb Queues) is a simple python3.5+
library for queueing jobs and processing them in workers using rethinkdb.


Features
--------

- Accepts *coroutines* as jobs
- Distributed
- Handles everything in a *rethinkdb database object* that can be reused
  at the worker
- Exposes changes via websockets
- Uses rethinkdb streaming "changes" method, wich is pretty efficient
- Uses asyncio, implements an async context manager for jobs and
  async iterator for the workers

::

                            +--> Worker
                            |
    Rethindb --> Dispatcher ---> Worker
                            |
                            +--> Worker




Usage
=====

Tqueues provides two entry points, tqueues-dispatcher and tqueues_worker.
Those have been previously explained. You'll need to point your workers to a
single dispatcher (that can be on the same machine or different machines).

To schedule a task, we must create it in the dispatcher, using its rest API,
you can check the rest API out at readthedocs:
