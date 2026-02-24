from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import anyio
from fastapi import BackgroundTasks

# Create a global thread pool executor
executor = ThreadPoolExecutor(max_workers=50)


def threaded_function(func):
    def wrapper(*args, **kwargs):
        executor.submit(func, *args, **kwargs)
    return wrapper


class GetBG:
    """
    context manager for fastapi.BackgroundTasks
    """

    def __init__(self):
        self.bg = BackgroundTasks()

    def __enter__(self):
        return self.bg

    def __exit__(self, exc_type, exc_value, traceback):
        # Still use a dedicated thread for background tasks execution to avoid blocking the executor with long running async tasks
        # or we could submit to executor if we are sure it won't deadlock
        Thread(target=anyio.run, args=(self.bg,), daemon=True).start()

    async def __aenter__(self):
        return self.bg

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.bg()
