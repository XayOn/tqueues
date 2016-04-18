#!/usr/bin/env python3.5
"""
    Tqueues - Simple queues management
"""

import sys
import json
import asyncio
import importlib
import aiohttp
from aiohttp import web
import rethinkdb as r
from rethinkdb.errors import ReqlOpFailedError, ReqlCursorEmpty


r.set_loop_type("asyncio")
RT_DB = "tqueues"


class WorkerDispatcher:
    """
        Implements an iterator over the HTTP Dispatcher API
    """
    def __init__(self, endpoint_url, queue):
        self.endpoint_url = endpoint_url
        self.queue = queue

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            with aiohttp.ClientSession() as session:
                async with session.get(self.endpoint_url,
                                       params={'queue': self.queue}) as resp:
                    assert resp.status == 200
                    return await resp.json()
        except AssertionError:
            pass


class Worker:
    """
        Iterates over the provided job dispatcher and
        tries to import and execute the given task.
        Dispatcher has to provide a dict with "method",
        "args" and "kwargs"
        This is actually done by splitting method
        wich should be in the form::

            foo.method
            foo.bar.method

        That would import foo and foo.bar and getattr(foo, 'method')
    """

    def __init__(self, dispatcher):
        assert isinstance(dispatcher, WorkerDispatcher)
        self.dispatcher = dispatcher

    async def run_forever(self):
        """
            We iterate over the job dispatcher object
            assigned to us.
        """
        async for data in self.dispatcher:
            try:
                module, method = data['method'].rsplit('.', 1)
                method = getattr(importlib.import_module(module), method)
                return method(*data['args'], **data['kwargs'])
            except:
                pass


class Dispatcher(web.View):
    """
        Exposes an API for the workers that run as simple consumers
        When a worker finishes its task, consumes the next one.

        Accepts ``rethinkdb.connect`` args.
        For a more secure environment, you should ONLY HAVE ONE DISPATCHER
    """

    async def get(self):
        """
            'pop' a task from the database
        """
        conn = await r.connect(**self.request.app['rethinkdb'])
        queue = r.db(RT_DB).table(self.request.GET['queue'])

        cursor = await queue.changes(include_initial=True).run(conn)
        await cursor.fetch_next()
        result = await cursor.next()

        dres = await queue.get(result['id']).delete().run(conn)
        if dres['deleted'] == 0:
            raise Exception("This task has already been consumed")

        return web.Response(text=json.dumps(result))

    async def post(self):
        """
            Creates a new task, just dumps whatever
            we have to the database after a brief format check

              {
                'queue': 'foo', method': 'method.to.execute',
                'args': (args), 'kwargs': {kwargs}
              }

              Note that method should be importable by the workers.

        """
        conn = await r.connect(**self.request.app['rethinkdb'])
        await self.request.post()

        mandatory = ['queue', 'args', 'kwargs', 'method']
        assert all([a in self.request.POST for a in mandatory])

        queue = r.db(RT_DB).table(self.request.POST['queue'])
        await queue.insert(self.request.POST).run(conn)
        return web.Response(body=b'ok')

    async def put(self):
        """ Creates a queue. """
        conn = await r.connect(**self.request.app['rethinkdb'])
        qname = self.request.GET['queue']
        r.db(RT_DB).table_create(qname).run(conn)
        return web.Response(body=b'ok')



def client(endpoint_url=False, queue=False):
    """ Starts a worker for a given endpoint_url and queue"""
    if not endpoint_url:
        endpoint_url, queue = sys.argv[1:]
    dispatcher = WorkerDispatcher(endpoint_url, queue)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Worker(dispatcher).run_forever())
    return loop.close()


def server(**kwargs):
    """ Starts main dispatching server """
    app = web.Application()
    app.router.add_route('*', '/', Dispatcher)
    app['rethinkdb'] = kwargs
    web.run_app(app)
