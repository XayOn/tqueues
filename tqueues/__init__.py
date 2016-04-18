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
            GET request to / with data queue=queue returns a job for
            the specified queue. It actually POPS the job from the db.
            If the database does not exists or there are no more tasks it
            waits for a change to happen in the database in a blocking way.
        """
        conn = await r.connect(**self.request.app['rethinkdb'])
        qname = self.request.GET['queue']
        queue = r.db(RT_DB).table(qname)

        async def get_next_from_cursor(cursor):
            """ Waits for result to be ready and returns it"""
            await cursor.fetch_next()
            return await cursor.next()

        async def get_changes():
            """ Waits for changes, gets one and returns it"""
            cursor = await queue.changes().run(conn)
            result = await get_next_from_cursor(cursor)
            return result['new_val']

        # This is a bit overcomplicated

        # It's like this to allow us to get the 'lost' tasks
        # on start, so, if someone pushes tasks to the database
        # or the server dies before dispatching everything,
        # those tasks will be the first to be consumed.

        # Then, when we finishe all available tasks (we 'sync')
        # it starts waiting for changes in its usual way
        try:
            cursor = await queue.run(conn)
            result = await get_next_from_cursor(cursor)
        except (ReqlCursorEmpty, StopIteration):
            result = await get_changes()
        except ReqlOpFailedError:
            r.db(RT_DB).table_create(qname).run(conn)
            result = await get_changes()

        try:
            result = await queue.get(result['id']).delete().run(conn)
            assert result['deleted'] == 1
        except (ReqlOpFailedError, AssertionError):
            raise Exception("This task has already been consumed")

        return web.Response(text=json.dumps(result))

    async def post(self):
        """
            Creates a new task, just dumps whatever
            we have in the database after a brief format check

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
