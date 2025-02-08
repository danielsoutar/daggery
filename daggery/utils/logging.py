import logging

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
