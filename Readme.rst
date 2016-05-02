Tqueues
=======

Tqueues (reThinkdb Queues) is a simple python3.5+ library for queueing jobs and processing them in workers.
Tqueues have a few advantages over other queuing solutions out there:

- Accepts coroutines as jobs
- Handles everything in a rethinkdb database object that can be reused at the worker 
  (I.E integrating it directly with a web interface via websockets)
- Uses rethinkdb streaming "changes" method, wich is pretty fast
- Workers use a simple http-based dispatcher, wich can be used to have
  multiple workers in different machines
- Uses asyncio, implements an async context manager for jobs and async iterator for the workers


On redis databases that is usually acomplished by delegating the dispatching to the database, so that
the database itself must be accesible from the worker machines. While that setup has its pros and cons, 
rethinkdb is not as atomic and may have race conditions when getting jobs, so an ideal setup is a simple
consumer on the rethinkdb side, and multiple consumers to that intermediate consumer, that is:


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
