Tqueues
=======

Simple rethinkdb-based job queues

This python3.5+ library provides with a simple interface to handle job executions using a rethinkdb 
database as backend.

For that, it implements a job dispatcher and and a worker. In addition to that, the job dispatcher can
be in a different machine that the jobs themselves.

On redis databases that is usually acomplished by delegating the dispatching to the database, so that
the database itself must be accesible from the worker machines.
While that setup has its pros and cons, rethinkdb is not as atomic and may have race conditions when
getting jobs, so an ideal setup is a simple consumer on the rethinkdb side, and multiple consumers to that
intermediate consumer, that is:


:: 

                            +--> Worker
                            |
    Rethindb --> Dispatcher ---> Worker
                            |
                            +--> Worker


Usage
=====

Tqueues provides two endpoints, tqueues-dispatcher and tqueues_worker.
Those have been previously explained.

To schedule a task, we must create it in the dispatcher, using its rest API 
