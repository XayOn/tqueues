#!/usr/bin/env python3.5
"""
    Tqueues - Simple queues management
"""

import os
import sys
import logging
import json
import asyncio
import inspect
import configparser
from contextlib import suppress
import importlib
import aiohttp
import aiohttp_cors
from aiohttp import web
import rethinkdb as r


logging.basicConfig(level=logging.DEBUG)
r.set_loop_type("asyncio")
RT_DB = "tqueues"
TASK_PENDING = 'pending'
TASK_FINISHED = 'finished'
TASK_STARTED = 'started'


async def test(*args, **kwargs):
    """ Test function for worker"""
    import datetime
    curr_data = kwargs['tq_parent_job'].data.copy()
    curr_data.update({'date_start':
                      datetime.datetime.today().strftime('%D %H:%M')})
    await kwargs['tq_parent_job'].update(curr_data)
    await asyncio.sleep(5)
    curr_data.update({'foo': 'bar'})
    await kwargs['tq_parent_job'].update(curr_data)
    return args, kwargs


class Job:
    """
        Job
    """
    def __init__(self, endpoint_url, data):
        self.endpoint_url = endpoint_url
        self.data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, value, tback):
        with aiohttp.ClientSession() as session:
            queue = self.data['queue']
            await session.delete(self.endpoint_url, params={
                'id': self.data["id"], 'queue': queue})

    async def update(self, data):
        """ Update remote object in the database directly """
        with aiohttp.ClientSession() as session:
            queue = self.data['queue']
            await session.patch(self.endpoint_url, params={
                "queue": queue, 'id': self.data['id']}, data=data)

    @property
    def method(self):
        """ Method """
        module, method = self.data['method'].rsplit('.', 1)
        method = getattr(importlib.import_module(module), method)
        return method

    async def work(self):
        """
            Run the job

            .. warning:: Job MUST accept a named parameter ``job``
            containing this object.
        """
        async with self:
            try:
                args, kwargs = self.data['args'], self.data['kwargs']
                kwargs.update({'tq_parent_job': self})
                if inspect.iscoroutinefunction(self.method):
                    return await self.method(*args, **kwargs)
                return self.method(*args, **kwargs)
            except:
                return False


class Worker:
    """
        Implements a simple worker over the http api
    """

    def __init__(self, endpoint_url, queue):
        self.endpoint_url = endpoint_url
        self._id = False
        self.queue = queue

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            with aiohttp.ClientSession() as session:
                args = (self.endpoint_url,)
                kwargs = {"params": {'queue': self.queue}}
                async with session.get(*args, **kwargs) as resp:
                    if resp.status == 501:
                        async with session.put(*args, **kwargs) as resp:
                            assert resp.status == 200
                    assert resp.status == 200
                    data = await resp.json()
                    return Job(self.endpoint_url, data)
        except AssertionError:
            await asyncio.sleep(2)

    async def run_forever(self):
        """
            We iterate over the job dispatcher object
            assigned to us.
        """
        async for job in self:
            if job:
                await job.work()


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
        conn = await r.connect(**self.request.app['rethinkdb'])
        queue = r.db(RT_DB).table(self.request.GET['queue'])
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
        conn = await r.connect(**self.request.app['rethinkdb'])
        await self.request.post()

        mandatory = ['queue', 'method']
        if not all([a in self.request.POST for a in mandatory]):
            raise web.HTTPBadRequest()

        queue = r.db(RT_DB).table(self.request.POST['queue'])
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
        conn = await r.connect(**self.request.app['rethinkdb'])
        qname = self.request.GET['queue']
        with suppress(r.errors.ReqlOpFailedError):
            r.db(RT_DB).table_create(qname).run(conn)

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
        conn = await r.connect(**self.request.app['rethinkdb'])
        queue = r.db(RT_DB).table(self.request.GET['queue'])
        res = await queue.get(self.request.GET['id']).update(
            {'status': TASK_FINISHED}).run(conn)
        assert res['replaced'] == 1
        return web.json_response(res)

    async def patch(self):
        """
           TODO: this should update something in the database
           The thing is we need to update data in the database
           from the job, and the job might not be able to
           access rerhinkdb for itself, so...
        """
        conn = await r.connect(**self.request.app['rethinkdb'])
        queue = r.db(RT_DB).table(self.request.GET['queue'])
        data = await self.request.post()
        res = await queue.get(self.request.GET['id']).update(data).run(conn)
        assert res['replaced'] == 1
        return web.json_response(res)


async def wshandler(request):
    """ Websocket handler """
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    conn = await r.connect(**request.app['rethinkdb'])
    queue = r.db(RT_DB).table(request.GET['queue'])
    cursor = await queue.changes(include_initial=True).run(conn)

    while cursor.fetch_next():
        ws.send_str(json.dumps(dict(await cursor.next())['new_val']))

    return ws


def client(endpoint_url=False, queue=False):
    """
        Starts a worker for a given endpoint_url and queue
        Calls ``Worker.run_forever``
    """
    if not endpoint_url:
        endpoint_url, queue = sys.argv[1:]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Worker(endpoint_url, queue).run_forever())
    return loop.close()


def server():
    """
       Starts main dispatching server

       Reads configuration from ~/.tqueues.conf
       Configuration must follow the format:

       [rethinkdb]
       any = param
       that = rethinkdb
       can = accept

       [cors]
       allowed_domains = a, list, of, domains (with protocol)
    """

    def get_config_as_dict(config):
        def _get_opts(section):
            for option in config.options(section):
                yield option, config.get(section, option)

        results = {}
        for section in config.sections():
            results[section] = dict(_get_opts(section))
        return results

    rethinkdb_opts = {}
    allowed_domains = []

    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.tqueues.conf'))
    config_ = get_config_as_dict(config)

    with suppress(KeyError):
        rethinkdb_opts = config_['rethinkdb']

    with suppress(KeyError):
        allowed_domains = config_['cors']['allowed_domains'].split(',')
        allowed_domains = [a.strip() for a in allowed_domains]

    app = web.Application()
    app['rethinkdb'] = rethinkdb_opts

    default_opts = aiohttp_cors.ResourceOptions(
        allow_credentials=True, expose_headers="*", allow_headers="*")
    cors = aiohttp_cors.setup(app, defaults={
        dom: default_opts for dom in allowed_domains})

    cors.add(cors.add(app.router.add_resource('/')).add_route('*', Dispatcher))
    cors.add(cors.add(app.router.add_resource('/ws')).add_route('*', wshandler))
    web.run_app(app)
