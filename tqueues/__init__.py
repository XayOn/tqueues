#!/usr/bin/env python3.5
"""
    Tqueues - Simple queues management
"""

import sys
import json
import asyncio
import inspect
from contextlib import suppress
import importlib
import aiohttp
from aiohttp import web
import rethinkdb as r


r.set_loop_type("asyncio")
RT_DB = "tqueues"
TASK_PENDING = 'pending'
TASK_FINISHED = 'finished'
TASK_STARTED = 'started'


def test(*args, **kwargs):
    """ Test function for worker"""
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
            await session.delete(self.endpoint_url, params={
                'id': self.data["id"]})

    @property
    def method(self):
        """ Method """
        module, method = self.data['method'].rsplit('.', 1)
        method = getattr(importlib.import_module(module), method)
        return method

    async def work(self):
        """ Run the job """
        async with self:
            args, kwargs = self.data['args'], self.data['kwargs']
            if inspect.iscoroutinefunction(self.method):
                return await self.method(*args, **kwargs)
            return self.method(*args, **kwargs)


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

        cursor = await queue.changes(include_initial=True).run(conn)
        await cursor.fetch_next()
        result = await cursor.next()

        # TODO: there is a race condition here. If the workers are
        # harassing the dispatcher and the jobs to execute are either
        # running real quick or failing, they may be reexecuted, as
        # this will be updated (to complete).
        # As a temporary workaround, I'm checking if the task is completed
        # just before getting it, it is ALMOST atomic, yet the race condition
        # may still happen...

        try:
            dres = await queue.get(result['id']).run(conn)
            assert dres['status'] != TASK_FINISHED
            dres = await queue.get(result['id']).update(
                {'status': TASK_STARTED}).run(conn)
            assert dres['updated'] == 0
        except AssertionError:
            raise web.HTTPNotFound("This task has already been consumed")

        return web.Response(text=json.dumps(result))

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

        mandatory = ['queue', 'args', 'kwargs', 'method']
        if not all([a in self.request.POST for a in mandatory]):
            raise web.HTTPBadRequest()

        queue = r.db(RT_DB).table(self.request.POST['queue'])
        data = dict(self.request.POST)
        data.update({'status': TASK_PENDING})

        try:
            await queue.insert(data).run(conn)
        except r.errors.ReqlOpFailedError as err:
            if 'does not exist.' in err.message:
                raise web.HTTPNotImplemented()
            raise web.HTTPNotFound('No more tasks')
        return web.Response(body=b'ok')

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
        .. http:delete:: /?id={string:id}

        Marks a task as completed

        **Example request**:

        .. sourcecode:: http

            DELETE /?id=foo
            Host: example.com
            Accept: application/json, text/javascript

        **Example response**:

        .. sourcecode:: http

            HTTP/1.1 200 OK
            Vary: Accept
            Content-Type: text/javascript

            ok

        :query id: id to mark as completed
        :statuscode 200: This method always should return 200


        """
        conn = await r.connect(**self.request.app['rethinkdb'])
        db_ = r.db(RT_DB)
        db_.get({'id': self.request.GET['id']}).update(
            {'status': TASK_FINISHED}).execute(conn)
        return web.Response(body=b'ok')


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


def server(**kwargs):
    """
       Starts main dispatching server
       Accepts any connection argument that rethinkdb accepts.
    """
    app = web.Application()
    app.router.add_route('*', '/', Dispatcher)
    app['rethinkdb'] = kwargs
    web.run_app(app)
