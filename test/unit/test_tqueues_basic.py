import pytest
from aiohttp import web


class TestWorker:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('id', list(range(10)))
    async def test_single_task(self, create_server, id):
        from tqueues import Worker

        self.deleted = False

        def get_response(id):
            """ Get response json """
            return {
                'queue': 'test',
                'args': [id],
                'kwargs': {'param': id},
                'method': 'tqueues.test',
                'id': id
            }

        response = get_response(id)

        async def handler(request):
            """
                Return always test task
            """
            return web.json_response(response)

        async def handler_del(request):
            """
                DELETED stuff
            """
            self.deleted = True
            return web.Response(body=b'OK')

        app, url = await create_server()
        app.router.add_route('GET', '/', handler)
        app.router.add_route('DELETE', '/', handler_del)
        worker = Worker(url, "test")

        async for job in worker:
            result = await job.work()
            job = result.pop('tq_parent_job')
            assert result == tuple([tuple(response['args']),
                                    response['kwargs']])
            assert self.deleted
            break

    @pytest.mark.asyncio
    @pytest.mark.parametrize('id', list(range(10)))
    async def test_multi_task(self, create_server, id):
        from tqueues import Worker

        self.deleted = False

        def get_response():
            """ Get response json """
            for id in range(10):
                yield {
                    'queue': 'test',
                    'args': [id],
                    'kwargs': {'param': id},
                    'method': 'tqueues.test',
                    'id': id
                }

        response = get_response()

        async def handler(request):
            """
                Return always test task
            """
            self.current_response = next(response)
            return web.json_response(self.current_response)

        async def handler_del(request):
            """
                DELETED stuff
            """
            self.deleted = True
            return web.Response(body=b'OK')

        app, url = await create_server()
        app.router.add_route('GET', '/', handler)
        app.router.add_route('DELETE', '/', handler_del)
        worker = Worker(url, "test")

        enum = 0
        async for job in worker:
            enum += 1
            res = await job.work()
            assert res == tuple([tuple(self.current_response['args']),
                                 self.current_response['kwargs']])
            assert self.deleted
            if enum == 9:
                break

    @pytest.mark.asyncio
    @pytest.mark.parametrize('id', list(range(10)))
    async def test_queue_creation(self, create_server, id):
        from tqueues import Worker

        self.deleted = False
        self.created = False

        def get_response():
            """ Get response json """
            for id in range(10):
                yield {
                    'queue': id,
                    'args': [id],
                    'kwargs': {'param': id},
                    'method': 'tqueues.test',
                    'id': id
                }

        response = get_response()

        async def handler(request):
            """
                Return always test task
            """
            raise web.HTTPNotImplemented()

        async def handler_put(request):
            """ PUT stuff """
            self.created = True
            return web.Response(body=b'OK')

        self.current_response = next(response)

        app, url = await create_server()
        app.router.add_route('GET', '/', handler)
        app.router.add_route('PUT', '/', handler_put)
        worker = Worker(url, self.current_response)

        enum = 0
        async for job in worker:
            enum += 1
            job
            assert self.created
            if enum == 9:
                break
