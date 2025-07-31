"""
logging.py
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains functions for logging data.
"""

import datetime
from io import TextIOWrapper
import os

from pulse.config import LOG_PATH


class MessageLogger:
    """This class holds the logging instance."""

    instance: TextIOWrapper
    """The Log file."""
    file_was_closed: bool
    """Whether the file has been closed."""

    @classmethod
    def open_log_file(cls, path: str) -> None:
        """This function initializes the log file."""
        file_path = LOG_PATH.format(path, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        cls.instance = open(file_path, "x", encoding="UTF-8")  # pylint: disable=consider-using-with

        cls.file_was_closed = False

    @classmethod
    def close_log_file(cls) -> None:
        """This function closes the log file."""
        if cls.file_was_closed:
            return

        file = cls.get_io_wrapper()

        file.flush()
        file.close()

        cls.file_was_closed = True

    @classmethod
    def get_io_wrapper(cls) -> TextIOWrapper:
        """This function gets the log file."""
        assert hasattr(cls, "instance")

        return cls.instance

    @classmethod
    def log(cls, message: str, prefix: str | None = None) -> None:
        """This function logs a message to the log file."""

        if prefix is None:
            string = message
        else:
            string = ""
            for line in message.splitlines():
                string += f"{prefix}: {line}\n"

        if not cls.file_was_closed:
            file = cls.get_io_wrapper()

            file.write(string)
            file.flush()


def open_log_file(path: str) -> None:
    """This function opens the log file."""
    MessageLogger().open_log_file(path)


def close_log_file() -> None:
    """This function closes the log files"""
    MessageLogger().close_log_file()


def log_info(message: str) -> None:
    """This function logs an info event."""
    MessageLogger().log(message, prefix="Info")


def log_warning(message: str) -> None:
    """This function logs a warning event."""
    MessageLogger().log(message, prefix="WARNING")


def log_error(message: str) -> None:
    """This function logs an error event."""
    MessageLogger().log(message, prefix="ERROR")


def log_critical(message: str) -> None:
    """This function logs a critical event."""
    MessageLogger().log(message, prefix="CRITICAL")
