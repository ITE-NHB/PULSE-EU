"""
ui.py
-----

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains functions for nicely formatted debugging.
"""

from functools import wraps
from time import time
from typing import Any, Sequence, Callable, ParamSpec, TypeVar


def print_percentage(percentage: float, fraction: bool = True) -> None:
    """This function replaces the last 7 characters with the percentage: "123.45%\" """
    if fraction:
        percentage *= 100

    percentage_string = f"{percentage:.2f}".rjust(6, " ")
    print("\b" * 7 + percentage_string + "%", end="", flush=True)


def format_number(number: int | float, long: bool = False) -> str:
    """Formats a number nicely."""
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}{" billion" if long else "b"}"
    if number >= 1_000_000:
        return f"{number / 1_000_000:.2f}{" million" if long else "m"}"
    if number >= 1_000:
        return f"{number / 1_000:.2f}{" thousand" if long else "k"}"
    return f"{number:.2f}" if isinstance(number, float) else str(number)


def format_time(seconds: float) -> str:
    """Formats time nicely."""

    string = ""

    seconds_per_day = 60 * 60 * 24
    seconds_per_hour = 60 * 60
    seconds_per_minute = 60

    days = int(seconds / seconds_per_day)
    seconds %= seconds_per_day

    hours = int(seconds / seconds_per_hour)
    seconds %= seconds_per_hour

    minutes = int(seconds / seconds_per_minute)
    seconds %= seconds_per_minute

    if days > 0:
        string += str(days) + " "

        if days == 1:
            string += "day "
        else:
            string += "days "

    if hours > 0:
        string += str(hours) + " "

        if hours == 1:
            string += "hour "
        else:
            string += "hours "

    if minutes > 0:
        string += str(minutes) + " "

        if minutes == 1:
            string += "minute "
        else:
            string += "minutes "

    string += f"{seconds:.2f} "

    if seconds == 1:
        string += "second"
    else:
        string += "seconds"

    return string


def format_list(items: Sequence[Any], sort: bool = False) -> str:
    """Formats the list nicely."""

    items_copy = list(items)

    if len(items_copy) == 0:
        return ""

    if sort:
        items_copy = sorted(items_copy)

    last = items_copy.pop()

    string = ", ".join(items_copy)

    if string != "":
        string = " and ".join([string, last])
    else:
        string = last

    return string


P = ParamSpec("P")
T = TypeVar("T")


def time_function(name: str, time_string: str = "calculation time") -> Callable[[Callable[P, T]], Callable[P, T]]:
    """This function times another function."""

    def decorator(function: Callable[P, T]) -> Callable[P, T]:
        @wraps(function)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            rt_start = time()
            result = function(*args, **kwargs)
            rt_end = time()

            rt_string = format_time(rt_end - rt_start)
            print(f"{name} {time_string}: {rt_string}\n")

            return result

        return wrapper

    return decorator


def parse_string_to_list(string: str) -> list[str] | None:
    """Parses a string into a list of strings."""
    string = string.strip()

    if string[0] != "[" or string[-1] != "]":
        return None

    string = string[1:-1]

    if len(string) == 0:
        return []

    elements = string.split(",")

    result: list[str] = []

    for element in elements:
        element = element.strip()

        if element[0] == element[-1] == "'" or element[0] == element[-1] == '"':
            element = element[1:-1]

        result.append(element)

    return result
