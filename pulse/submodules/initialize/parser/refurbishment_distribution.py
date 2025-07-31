"""
refurbishment_distribution.py
-----------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The refurbishment distribution parser.
"""

from pulse.config import REFURBISHMENT_DISTRIBUTION_PATH, RENOVATION_DISTRIBUTION_RAW_DATA
from pulse.support.file_interaction import write_dict_to_json, load_dict_from_json


def get_refurbishment_distribution() -> dict[str, dict[str, float]]:
    """Gets a fixed refurbishment rate specified in the function for every country."""
    return load_dict_from_json(RENOVATION_DISTRIBUTION_RAW_DATA)


def parse_refurbishment_distribution() -> None:
    """This function parses the refurbishment rates for these countries."""

    data = get_refurbishment_distribution()

    write_dict_to_json(data, REFURBISHMENT_DISTRIBUTION_PATH)
