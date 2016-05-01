import pytest
import asyncio
from aiohttp import web


@pytest.yield_fixture
def create_server(event_loop):
    """ Returns a server"""
    app = handler = srv = None
    loop = event_loop

    @asyncio.coroutine
    def create(*, debug=False, ssl_ctx=None, proto='http'):
        nonlocal app, handler, srv
        app = web.Application(loop=loop)
        port = 8000
        handler = app.make_handler(debug=debug, keep_alive_on=False)
        srv = yield from loop.create_server(handler, '127.0.0.1', port,
                                            ssl=ssl_ctx)
        if ssl_ctx:
            proto += 's'
        url = "{}://127.0.0.1:{}".format(proto, port)
        return app, url

    yield create

    @asyncio.coroutine
    def finish():
        yield from handler.finish_connections()
        yield from app.finish()
        srv.close()
        yield from srv.wait_closed()

    loop.run_until_complete(finish())
