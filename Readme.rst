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

TQueues worker
--------------

::

    Usage:
    	tqueues_worker -h | --help
    	tqueues_worker --version
    	tqueues_worker --endpoint_url <endpoint_url>
    	tqueues_worker --queue <queue>

    Options:
    	-h --help                        Show this screen
    	-v --version                     Show version
    	--endpoint_url <ENDPOINT_URL>    TQueues dispatcher endpoint
    	--queue        <QUEUE>           Endpoint queue to listen on

    Examples:
    	tqueues_worker --endpoint_url http://127.0.0.1:800/ --queue testqueue


Tqueues job dispatcher
----------------------

::
    Usage:
        tqueues_dispatcher --db "db" --host "127.0.0.1" --port 28015
        tqueues_dispatcher --db "db" --host "127.0.0.1"
        tqueues_dispatcher --db "db" --port 28015
        tqueues_dispatcher --db "db" --user 'user'
        tqueues_dispatcher --db "db" --password 'password'
        tqueues_dispatcher -h | --help
        tqueues_dispatcher --version

    Options:
        --host "127.0.0.1"                          Rethinkdb host
        --db "db"                                   Rethinkdb databaes
        --port 28015                                Rethinkdb port
        --user 'user'                               Rethinkdb user
        --password 'password'                       Rethinkdb password
        --allowed_domains 'foo.com,bar.com'         Allowed domains
        --loglevel (DEBUG|INFO)                     Loglevel
        -h   --help                                 Show this screen
        --version                                   Show version

    Examples:
        tqueues_dispatcher --host localhost --db foo --port 28015 --user foo --password bar --loglevel INFO --allowed_domains 'foo.com,bar.com'
