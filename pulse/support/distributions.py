"""
distributions.py
----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains distribution functions and a wrapper to for the generic function to use.
"""

from typing import TypeVar

from pulse.support.debug_flags import CHECK_DISTRIBUTE_DATA_INTEGRITY
from pulse.support.logging import log_warning


T = TypeVar("T")


def distribute(count: int, probabilities: dict[T, float]) -> dict[T, int]:
    """
    This function distributes count integer values into buckets specified in probabilities. It loses some accuracy.
    """
    # Initial approximation
    distributed = {key: int(d * count + 0.5) for key, d in probabilities.items()}

    difference = sum(distributed.values()) - count

    # If perfectly distributed, just return
    if difference == 0:
        return distributed

    # This sorts the distribution probabilities in descending order, highest probabilities coming first
    ordered = list(dict(sorted(probabilities.items(), key=lambda item: item[1], reverse=True)).keys())

    multiplier = 1
    if difference > 0:
        multiplier = -1

    for i in range(abs(difference)):
        index = ordered[i % len(ordered)]

        assert distributed[index] + multiplier >= 0, "Trying to subtract from 0 where it shouldnt be done"
        distributed[index] += multiplier

    if CHECK_DISTRIBUTE_DATA_INTEGRITY:
        # Totaling the buildings again to check if all of them are distributed properly
        counted_buildings = 0

        for value in distributed.values():
            assert (
                isinstance(value, int) and value >= 0
            ), f"The distributed values are not positive integers! type({value}) = {type(value)}"

            counted_buildings += value

        assert (
            counted_buildings == count
        ), f"The distributed buildings don't add up to the total buildings! Buildings: {counted_buildings}/{count}"

    return distributed


def distribute_fully(
    count: int,
    distribution: dict[T, int] | dict[T, float],
    normalize: bool = False,
    default: T | None = None,
) -> dict[T, int]:
    """The distribution function to use if there is no need for a specific version."""
    local_distribution = distribution

    if default is not None:
        assert default in local_distribution, f"Trying to set invalid default {default} in {local_distribution}."

    if count == 0:
        return {code: 0 for code in local_distribution}

    if sum(local_distribution.values()) == 0:
        if default is not None:
            local_distribution[default] = 1
        else:
            log_warning(f"Bad data in distribute_fully(): distribute {count} across {local_distribution}")
            return {code: 0 for code in local_distribution}

    if normalize:
        local_distribution = normalize_distribution(mapping=local_distribution)
    else:
        local_distribution = {
            key: (value if isinstance(value, float) else float(value)) for key, value in local_distribution.items()
        }

    assert (
        round(sum(local_distribution.values()), 8) == 1
    ), f"The probabilities dictionary sums to {sum(local_distribution.values())}!"

    return distribute(count, local_distribution)


def normalize_distribution(mapping: dict[T, float] | dict[T, int]) -> dict[T, float]:
    """This function normalizes the dictionary to map to a total probability of 1."""
    total = sum(mapping.values()) or 1

    distribution = {key: value / total for key, value in mapping.items() if value > 0}

    return distribution


def distribute_fully_capped(
    count: int,
    distribution: dict[T, int] | dict[T, float],
    caps: dict[T, int],
) -> tuple[dict[T, int], dict[T, int], int]:
    """This function distributes count items into the buckets specified in distribution, while respecting a maximum
    amount for each bucket.\n
    Arguments:
        count (int): The amount of items to distribute.
        distribution (dict[str, float]): The distribution function, mapping from a key to its probability.
        caps: (dict[str, int]): The maximum caps per bucket.

    Returns:
        (tuple[dict[str, int], dict[str, int], int]):
        A tuple containing the distributed items in the first index, the leftovers of the caps in the second index,
        and the overflow in the third index
    """
    if len(distribution) == 0 or count <= 0:
        return ({}, caps, count)

    local_distribution = distribution
    distributed = {key: 0 for key, prob in distribution.items() if prob != 0}

    new_caps = caps  # copy.deepcopy(caps)

    if CHECK_DISTRIBUTE_DATA_INTEGRITY:
        assert all(cap >= 0 for cap in new_caps.values())

    overflow = count

    while overflow > 0:
        local_distribution = normalize_distribution(local_distribution)

        # Distribute all
        new_distributed = distribute_fully(count=overflow, distribution=local_distribution)

        old_overflow = overflow
        overflow = 0

        # Check for overflows
        for key, value in new_distributed.items():
            cap = new_caps[key]

            if value >= cap:
                overflow += value - cap

                local_distribution.pop(key)

                value = cap

            distributed[key] += value
            new_caps[key] -= value

        # Check whether no changes happened to overflow or no distribution buckets are left (all buckets full)
        if old_overflow == overflow or (len(local_distribution) == 0 and overflow != 0):
            break

    overflow = count - sum(distributed.values())

    return (distributed, new_caps, overflow)
