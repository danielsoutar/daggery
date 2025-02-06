import logging
import time
from functools import wraps

import colorlog


def logger_factory(name: str) -> logging.Logger:
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
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            logger.info(f"{self.name}:")
            logger.info(f"  args: {args}")
            logger.info(f"  kwargs: {kwargs}")
            return method(self, *args, **kwargs)

        return wrapper

    return decorator


def timed(logger):
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
    def decorator(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if len(args) > 0 and any(isinstance(arg, error_types) for arg in args):
                logger.info(f"{self.name} bypassed.")
                return args[0]
            return method(self, *args, **kwargs)

        return wrapper

    return decorator
