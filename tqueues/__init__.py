#!/usr/bin/env python3.5
"""
    Tqueues - Simple queues management
"""

import sys
import json
import asyncio
import inspect
import importlib
import aiohttp
from aiohttp import web
import rethinkdb as r


r.set_loop_type("asyncio")
RT_DB = "tqueues"


def test(*args, **kwargs):
    """ Test function for worker"""
    return all([('1' in args), (kwargs["1"] == "2")])


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

    def work(self):
        """ Run the job """
        args, kwargs = self.data['args'], self.data['kwargs']
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
                async with session.get(self.endpoint_url,
                                       params={'queue': self.queue}) as resp:
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
            with job:
                res = job.work()
                if inspect.iscoroutine(res):
                    asyncio.ensure_future(res)


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

        dres = await queue.get(result['id']).update(
            {'status': 'started'}).run(conn)
        if dres['updated'] == 0:
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
        data = dict(self.request.POST)
        data.update({'status': 'pending'})
        await queue.insert(data).run(conn)
        return web.Response(body=b'ok')

    async def put(self):
        """ Creates a queue. """
        conn = await r.connect(**self.request.app['rethinkdb'])
        qname = self.request.GET['queue']
        r.db(RT_DB).table_create(qname).run(conn)
        return web.Response(body=b'ok')

    async def delete(self):
        """ Marks a task as completed """
        conn = await r.connect(**self.request.app['rethinkdb'])
        db_ = r.db(RT_DB)
        db_.get({'id': self.request.GET['id']}).update(
            {'status': 'finished'}).execute(conn)
        return web.Response(body=b'ok')


def client(endpoint_url=False, queue=False):
    """ Starts a worker for a given endpoint_url and queue"""
    if not endpoint_url:
        endpoint_url, queue = sys.argv[1:]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Worker(endpoint_url, queue).run_forever())
    return loop.close()


def server(**kwargs):
    """ Starts main dispatching server """
    app = web.Application()
    app.router.add_route('*', '/', Dispatcher)
    app['rethinkdb'] = kwargs
    web.run_app(app)
