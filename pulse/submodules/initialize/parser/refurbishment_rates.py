"""
refurbishment_rates.py
----------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The refurbishment rates parser.
"""

from pulse.config import REFURBISHMENT_RATES_DATA
from pulse.support.defines import DEEP, LIGHT, MEDIUM, NON_RESIDENTIAL, RESIDENTIAL
from pulse.support.file_interaction import write_dict_to_json


def get_fixed_ref_rates() -> dict[str, dict[str, float]]:
    """Gets a fixed refurbishment rate specified in the function for every country."""
    return {
        RESIDENTIAL: {
            LIGHT: 0.008,
            MEDIUM: 0.0015,
            DEEP: 0.0005,
        },
        NON_RESIDENTIAL: {
            LIGHT: 0.008,
            MEDIUM: 0.0015,
            DEEP: 0.0005,
        },
    }


def parse_refurbishment_rates(countries: list[str]) -> None:
    """This function parses the refurbishment rates for these countries."""

    data: dict[str, dict[str, dict[str, float]]] = {}

    for country in countries:
        data[country] = get_fixed_ref_rates()

    write_dict_to_json(data, REFURBISHMENT_RATES_DATA)
