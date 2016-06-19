"""

TQueues worker

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

"""

from docopt import docopt
import aiohttp
import asyncio
import inspect
import importlib


def client():
    """
        Client
    """
    opts = docopt(__doc__, version="0.0.1")

    endpoint_url = opts['endpoint_url']
    queue = opts['queue']

    loop = asyncio.get_event_loop()
    loop.run_until_complete(Worker(endpoint_url, queue).run_forever())
    return loop.close()


class Job:
    """
        Context manager that handles a job on the dispatcher.

        This executes the given method (see `Job.method`) adding
        itself to it via "tq_parent_job" keyword argument.
        That means your worker methods need to accept a tq_parent_job.

        That makes the job able to update information in the rethinkdb
        using the update method of tq_parent_job.

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
        """
            Update remote object in the database providing `data`

            .. TODO::
                Implement #2
        """
        with aiohttp.ClientSession() as session:
            queue = self.data['queue']
            await session.patch(self.endpoint_url, params={
                "queue": queue, 'id': self.data['id']}, data=data)

    @property
    def method(self):
        """
            Finds and imports the method specified in an importable format
            (foo.bar:baz would use baz from foo.bar)
        """
        module, method = self.data['method'].rsplit(':', 1)
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
