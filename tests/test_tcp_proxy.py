import asyncio
import contextlib

from .conftest import TcpProxy


async def test_cancelled_pipe_does_not_break_connection_closure() -> None:
    server = await asyncio.start_server(lambda _, __: None, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    try:
        _, writer = await asyncio.open_connection(host="127.0.0.1", port=port)
        reader = asyncio.StreamReader()
        reader.feed_eof()

        proxy = TcpProxy(src_port=0, dst_port=port)
        pipe = asyncio.create_task(proxy._pipe(reader, writer))  # pyright: ignore[reportPrivateUsage]
        await asyncio.sleep(0)
        pipe.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pipe

        await writer.wait_closed()
    finally:
        server.close()
