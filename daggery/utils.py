import logging
import time
from functools import wraps

import colorlog
import requests


def _logger_factory(name: str) -> logging.Logger:
    # Create a logger instance
    logger = logging.getLogger(name)

    logger.setLevel(logging.INFO)

    # Create a console handler and set the level to INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Create a color formatter and set it for the handler
    formatter = colorlog.ColoredFormatter(
        fmt="%(log_color)s%(levelname)s%(reset)s: %(asctime)s [%(name)s]  %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    return logger


def logged(logger):
    """
    This is a simple example of a logging decorator for a Node.
    It `wraps` the enclosing method for niceties like debugging
    and logging. It logs the name of the node, the inputs, and
    the output.
    """

    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            logger.info(f"{self.name}:")
            logger.info(f"  args: {args}")
            logger.info(f"  kwargs: {kwargs}")
            output = method(self, *args, **kwargs)
            logger.info(f"  Output: {output}")
            return output

        return wrapper

    return decorator


def timed(logger):
    """
    This is a simple example of a timing decorator for a Node.
    It `wraps` the enclosing method for niceties like debugging
    and logging. It logs the duration of the node execution time.
    """

    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            start = time.time()
            result = method(self, *args, **kwargs)
            raw_duration = time.time() - start
            duration = round(raw_duration, ndigits=5)
            logger.info(f"{self.name} duration: {duration}s")
            return result

        return wrapper

    return decorator


def bypass(error_types, logger):
    """
    This is a simple example of a bypass decorator for a Node.
    It `wraps` the enclosing method for niceties like debugging
    and logging. It 'bypasses' or skips calling the underlying
    method if any of the inputs match one of the given error
    types. This is similar to the use of `bind` in FP-style
    languages, as Operations do not need to deal with errors
    from other Operations.

    However, only the first error will be propagated. Handling
    multiple errors and transforming them either requires a
    custom decorator or Operation.
    """

    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            # kwargs are not propagated by Operations, so
            # just checking args is sufficient.
            if any(isinstance(arg, error_types) for arg in args):
                logger.info(f"{self.name} bypassed.")
                # If multiple errors, return the first one.
                # TODO: Consider whether multiple outputs should be supported.
                return next(filter(lambda a: isinstance(a, error_types), args))
            return method(self, *args, **kwargs)

        return wrapper

    return decorator


def http_client(base_url):
    """
    This is a simple example of a HTTP client decorator for a Node.
    It `wraps` the enclosing method for niceties like debugging
    and logging. It implements dependency injection and provides a
    configured HTTP client.
    """

    def client(ep, pl):
        return requests.post(base_url + ep, json=pl)

    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            return method(self, *args, client, **kwargs)

        return wrapper

    return decorator
