import time
import asyncio
from functools import wraps
from .logger import logger

def profile_microseconds(func):
    """
    A decorator that profiles the execution time of a function in microseconds
    and logs the result. It can handle both synchronous and asynchronous functions.
    """

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start_time = time.perf_counter_ns()
        result = await func(*args, **kwargs)
        end_time = time.perf_counter_ns()
        # elapsed_us = (end_time - start_time) / 1000
        # Logging removed as per user request
        return result

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        start_time = time.perf_counter_ns()
        result = func(*args, **kwargs)
        end_time = time.perf_counter_ns()
        # elapsed_us = (end_time - start_time) / 1000
        # Logging removed as per user request
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper
