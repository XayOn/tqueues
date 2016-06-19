#!/usr/bin/env python3.5
"""
Tqueues job dispatcher.

Usage:
    tqueues_dispatcher --db "db" --queue "queue" --host "127.0.0.1"
    tqueues_dispatcher --db "db" --queue "queue" --port 28015
    tqueues_dispatcher --db "db" --queue "queue" --user 'user'
    tqueues_dispatcher --db "db" --queue "queue" --password 'password'
    tqueues_dispatcher --db "db" --queue "queue" --loglevel INFO
    tqueues_dispatcher --db "db" --queue "queue"
    tqueues_dispatcher -h | --help
    tqueues_dispatcher --version

Options:
    --host         <host>                       Rethinkdb host
    --db           <db>                         Rethinkdb databaes
    --port         <port>                       Rethinkdb port
    --user         <user>                       Rethinkdb user
    --password     <password>                   Rethinkdb password
    --allowed_domains 'foo.com,bar.com'         Allowed domains
    --loglevel (DEBUG|INFO)                     Loglevel
    -h   --help                                 Show this screen
    --version                                   Show version

Examples:
    tqueues_dispatcher --host localhost --db foo --port 28015 --user foo \
--password bar --loglevel INFO --allowed_domains 'foo.com,bar.com'

"""

from docopt import docopt
import logging
import json
from contextlib import suppress
import aiohttp_cors
from aiohttp import web
import rethinkdb as r


TASK_PENDING = 'pending'
TASK_FINISHED = 'finished'
TASK_STARTED = 'started'


class Dispatcher(web.View):
    """
        Exposes an API for the workers that run as simple consumers
        When a worker finishes its task, consumes the next one.

        Accepts ``rethinkdb.connect`` args.
        For a more secure environment, you should ONLY HAVE ONE DISPATCHER
        for database in rethinkdb.
    """

    async def get(self):
        """
        .. http:get:: /?queue={string:queue}

            Gets a task from the dispatcher.

            If we run into a race condition, it'll return a
            404 for the client to retry

        **Example request**:

        .. sourcecode:: http

            GET /?queue=foo
            Host: example.com
            Accept: application/json, text/javascript

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: text/javascript
            {
              'queue': 'foo', method': 'method.to.execute',
              'args': (*args), 'kwargs': {**kwargs}
            }

        .. note::

            method.to.execute must be a method importable in the workers' side

        :query queue: queue (table) where to listen in the database
        :statuscode 200: no error
        :statuscode 404: Race condition happened, task is no longer present
        """
        opts = self.request.app['rethinkdb']
        conn = await r.connect(**opts)
        queue = r.db(opts['db']).table(self.request.GET['queue'])
        filter_ = queue.filter({'status': TASK_PENDING})

        cursor = await filter_.changes(include_initial=True).run(conn)
        await cursor.fetch_next()
        result = await cursor.next()

        try:
            new_id = result['new_val']['id']
            dres = await queue.get(new_id).run(conn)
            assert dres['status'] != TASK_FINISHED
            assert dres['status'] != TASK_STARTED
            dres = await queue.get(new_id).update(
                {'status': TASK_STARTED}).run(conn)
            assert dres['replaced'] != 0
        except AssertionError:
            logging.debug('Task is being consumed already')
            raise web.HTTPNotFound()

        return web.Response(text=json.dumps(result['new_val']))

    async def post(self):
        """
            Creates a new task, just dumps whatever
            we have to the database after a brief format check

              {
                'queue': 'foo', method': 'method.to.execute',
                'args': (args), 'kwargs': {kwargs}
              }

        .. http:post:: /

            Gets a task from the dispatcher.

            If the table does not exist, it returns a 501 for
            the client to handle it

        **Example request**:

        .. sourcecode:: http

            POST /
            Host: example.com
            Accept: application/json, text/javascript

            {
              'queue': 'foo', method': 'method.to.execute',
              'args': (*args), 'kwargs': {**kwargs}
            }


        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: text/javascript

            ok

        .. note::

            method.to.execute must be a method importable in the workers' side

        :<json string queue: Queue (table) to add this task to
        :<json array args: List of positional arguments to pass to method
        :<json array kwargs: List of keyword arguments to pass to method
        :<json string method: Method to import and execute

        :statuscode 200: No error
        :statuscode 501: Table does not exist
        :statuscode 400: Not all params have been specified
        :statuscode 404: No more tasks in the queue, retry later

        """
        opts = self.request.app['rethinkdb']
        conn = await r.connect(**opts)
        await self.request.post()

        mandatory = ['queue', 'method']
        if not all([a in self.request.POST for a in mandatory]):
            raise web.HTTPBadRequest()

        queue = r.db(opts['db']).table(self.request.POST['queue'])
        data = dict(self.request.POST)
        if 'args' not in data:
            data['args'] = []
        if 'kwargs' not in data:
            data['kwargs'] = {}
        data.update({'status': TASK_PENDING})

        try:
            id_ = await queue.insert(data).run(conn)
        except r.errors.ReqlOpFailedError as err:
            if 'does not exist.' in err.message:
                raise web.HTTPNotImplemented()
            raise web.HTTPNotFound('No more tasks')
        data.update({'id': id_['generated_keys'][0]})
        return web.json_response(data)

    async def put(self):
        """
        .. http:put:: /?queue={string:queue}

            Creates a queue if it does not exist.

        **Example request**:

        .. sourcecode:: http

            GET /?queue=foo
            Host: example.com
            Accept: application/json, text/javascript

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: text/javascript

            ok

        :query queue: queue (table) to create
        :statuscode 200: This method always should return 200

        """
        opts = self.request.app['rethinkdb']
        conn = await r.connect(**opts)
        qname = self.request.GET['queue']
        with suppress(r.errors.ReqlOpFailedError):
            r.db(opts['db']).table_create(qname).run(conn)

        return web.Response(body=b'ok')

    async def delete(self):
        """
        .. http:delete:: /?queue={string:queue}&id={string:id}

        Marks a task as completed

        **Example request**:

        .. sourcecode:: http

            DELETE /?id=foo&queue=bar
            Host: example.com
            Accept: application/json, text/javascript

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: text/javascript

            ok

        :query id: id to mark as completed
        :query queue: queue (table) to work on
        :statuscode 200: This method always should return 200


        """
        opts = self.request.app['rethinkdb']
        conn = await r.connect(**opts)
        queue = r.db(opts['db']).table(self.request.GET['queue'])
        res = await queue.get(self.request.GET['id']).update(
            {'status': TASK_FINISHED}).run(conn)
        assert res['replaced'] == 1
        return web.json_response(res)

    async def patch(self):
        """

            In case the job is unable to access directly rethinkdb, we
            can use this endpoint to update the data.
            See `Job` documentation on `Job.update`.
        """
        opts = self.request.app['rethinkdb']
        conn = await r.connect(**opts)
        queue = r.db(opts['db']).table(self.request.GET['queue'])
        data = await self.request.post()
        res = await queue.get(self.request.GET['id']).update(data).run(conn)
        assert res['replaced'] == 1
        return web.json_response(res)


async def wshandle(request):
    """
        Websocket handler
        Right now this only waits on given queue and dumps
        'new_val' on rethinkdb changes.

        .. TODO:: #5
    """
    ws_ = web.WebSocketResponse()
    await ws_.prepare(request)

    opts = request.app['rethinkdb']
    conn = await r.connect(**opts)
    queue = r.db(opts['db']).table(request.GET['queue'])
    cursor = await queue.changes(include_initial=True).run(conn)

    while cursor.fetch_next():
        ws_.send_str(json.dumps(dict(await cursor.next())['new_val']))

    return ws_


def server():
    """
        Starts main dispatch server
    """
    allowed_domains = []

    opts = docopt(__doc__, version="0.0.1")

    logging.basicConfig(level=getattr(logging, opts.pop('loglevel')))
    r.set_loop_type("asyncio")

    with suppress(KeyError):
        allowed_domains = opts.pop('allowed_domains').split(',')
        allowed_domains = [a.strip() for a in allowed_domains]

    app = web.Application()
    app['rethinkdb'] = opts

    default_opts = aiohttp_cors.ResourceOptions(
        allow_credentials=True, expose_headers="*", allow_headers="*")
    cors = aiohttp_cors.setup(app, defaults={
        dom: default_opts for dom in allowed_domains})

    cors.add(cors.add(app.router.add_resource('/')).add_route('*', Dispatcher))
    cors.add(cors.add(app.router.add_resource('/ws')).add_route('*', wshandle))
    web.run_app(app)
