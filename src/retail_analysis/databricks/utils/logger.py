import logging

_CONFIGURED = False

def _configure_logging():
    global _CONFIGURED

    if _CONFIGURED:
        return

    fmt = logging.Formatter(
        "%(levelname)s | %(asctime)s | %(name)s | %(message)s"
    )

    root = logging.getLogger()

    # Apply formatter to all existing handlers
    for handler in root.handlers:
        handler.setFormatter(fmt)

    root.setLevel(logging.INFO)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    _configure_logging()
    return logging.getLogger(name)