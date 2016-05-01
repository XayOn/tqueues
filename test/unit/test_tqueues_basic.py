import pytest
from aiohttp import web


@pytest.mark.asyncio
async def test_tqueue_worker_can_process_a_normal_task(create_server):
    from tqueues import Worker

    response = {
        'queue': 'test',
        'args': ["1"],
        'kwargs': {"1": "2"},
        'method': 'tqueues.test',
        'id': 1
    }

    async def handler(request):
        """
            Return always test task
        """
        return web.json_response(response)

    async def handler_del(request):
        """
            DELETED stuff
        """
        return web.Response(body=b'OK')

    app, url = await create_server()
    app.router.add_route('GET', '/', handler)
    app.router.add_route('DELETE', '/', handler_del)
    worker = Worker(url, "test")

    async for job in worker:
        async with job:
            result = job.work()
            assert result is True
            break
