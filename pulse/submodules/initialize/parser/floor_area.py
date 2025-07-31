"""
floor_area.py
-------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The floor area parser.
"""

import json

import polars as pl

from pulse.config import (
    FLOOR_AREA_STATISTIC_PATH,
    FLOOR_AREA_INCREASE_RAW_DATA,
    POPULATION_STATISTIC_PATH,
    ARCHETYPE_STOCK_DATA_PATH,
)
from pulse.support.defines import RESIDENTIAL_TYPOLOGIES
from pulse.support.file_interaction import write_dict_to_json


def get_floor_area_from_stock(country: str) -> dict[int, float]:
    """This gets the floor area data for a given country from the alternate file from Nicolas."""

    def lerp(start: float, end: float, factor: float) -> float:
        return start + (end - start) * factor

    with open(FLOOR_AREA_INCREASE_RAW_DATA, mode="r", encoding="UTF-8") as file:
        increase_data: dict[str, dict[str, float]] = json.load(file)

    with open(POPULATION_STATISTIC_PATH.format(country), mode="r", encoding="UTF-8") as file:
        pop_2019: int = json.load(file)["2019"]

    relevant_columns = [
        "archetype_name",
        "number of buildings",
        "reference building useful floor area",
        "occupied",
        "vacant",
        "secondary dwellings/units and others",
    ]

    epoch_area_sums = (
        pl.read_parquet(ARCHETYPE_STOCK_DATA_PATH.format(country), columns=relevant_columns)
        .filter(pl.col("archetype_name").str.contains_any(RESIDENTIAL_TYPOLOGIES))
        .with_columns(
            epoch=pl.col("archetype_name").str.slice(7, 9),
            summed=(
                pl.col("number of buildings")
                * pl.col("reference building useful floor area")
                * pl.col("occupied")
                / (pl.col("occupied") + pl.col("vacant") + pl.col("secondary dwellings/units and others"))
            ),
        )
        .select("epoch", "summed")
        .group_by("epoch")
        .sum()
    )

    sqm_2019 = 0.0

    for row in epoch_area_sums.iter_rows(named=True):
        epoch = row["epoch"]
        summed = row["summed"]
        assert isinstance(epoch, str)
        assert isinstance(summed, float)

        start = int(epoch[:4])
        end = int(epoch[-4:])

        percentage = min(max(0, (2019 - start) / (end - start)), 1)
        sqm_2019 += percentage * summed

    sqm_2019 /= pop_2019

    sqm_2050_inc = increase_data["increase_to_2050"][country]

    all_sqm: dict[int, float] = {}
    sqm_2050 = sqm_2019 * (1 + sqm_2050_inc)

    for year in range(2019, 2050 + 1):
        t = (year - 2019) / (2050 - 2019)
        all_sqm[year] = lerp(sqm_2019, sqm_2050, t)

    return all_sqm


def generate_floor_areas(countries: list[str]) -> None:
    """This function calculates the floor area evolutions for these countries."""
    for country in countries:
        floor_area_data = get_floor_area_from_stock(country)

        write_dict_to_json(floor_area_data, FLOOR_AREA_STATISTIC_PATH.format(country.lower()))
