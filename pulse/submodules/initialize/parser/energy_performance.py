"""
energy_performance.py
---------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The energy performance parser.
"""

import polars as pl

from pulse.config import CONSTRUCTION_EP_RATES_PATH, CONSTRUCTION_EP_RATES_RAW_DATA
from pulse.support.file_interaction import write_dict_to_json

CONSTRUCTION_EP_TRANSLATION = {
    "NEW-STD": "1",
    "NEW-ADV": "2",
    "NEW-SS": "3",
}


def parse_energy_performance(countries: list[str]) -> None:
    """This function parses the energy performance rates for these countries."""
    df = pl.read_csv(CONSTRUCTION_EP_RATES_RAW_DATA)

    data: dict[str, dict[str, float]] = {}

    for country in countries:
        filter_data = df.filter(pl.col("Country") == country).select(pl.exclude("Country")).to_dicts()
        assert len(filter_data) != 0, f"Unable to get construction efficiency data for {country}"
        assert len(filter_data) == 1, f"Found multiple matches for {country}: {filter_data}"

        country_data: dict[str, float] = {}
        for key, val in filter_data[0].items():
            assert isinstance(val, float)
            country_data[CONSTRUCTION_EP_TRANSLATION[key]] = val

        country_data.pop("1", None)

        country_data["1"] = 1 - sum(country_data.values())
        assert country_data["1"] >= 0, f"The data adds up to more than 1: {1 - country_data["01"]}"

        data[country] = country_data

    write_dict_to_json(data, CONSTRUCTION_EP_RATES_PATH)
