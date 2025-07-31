"""
construction_rates.py
---------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The reference construction rates parser.
"""

import polars as pl

from pulse.config import CONSTRUCTION_RATES_NON_RES_RAW_DATA, CONSTRUCTION_RATES_NON_RES_PATH
from pulse.support.file_interaction import write_dict_to_json


def parse_construction_rates(countries: list[str]) -> None:
    """This function parses the construction permits for these countries."""
    con_area_data = pl.read_csv(CONSTRUCTION_RATES_NON_RES_RAW_DATA)

    con_rates_dict: dict[str, float] = dict(zip(con_area_data.to_series(0), con_area_data.to_series(1)))

    con_rates_dict = {country: con_rates_dict[country] for country in countries}

    write_dict_to_json(con_rates_dict, CONSTRUCTION_RATES_NON_RES_PATH)
