"""
calculate_activity_ratios.py
----------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the function to calculate activity ratios for different building types.
"""

import polars as pl

from pulse.activities import BuildingStock, Scenario
from pulse.support.defines import NON_RESIDENTIAL_TYPOLOGIES, RESIDENTIAL_TYPOLOGIES

from .defines import (
    DEMOLITION,
    CONSTRUCTION,
    REFURBISHMENT,
    RESIDENTIAL,
    COMMERCIAL,
    RES_AND_COMM,
)


def get_name_variable(comm_or_res: str, use: str) -> str:
    """This function gets the name variable for the pyam file."""
    return f"Energy Service|{comm_or_res}|{use} rate"


def calculate_rate(
    scenario: Scenario, building_stock: BuildingStock, use: str, comm_or_res: str, is_generation_run: bool
) -> pl.DataFrame:
    """This function constructs the file paths and calls the calculate_demolition_rate function."""
    typologies_filter: list[str] | None

    match comm_or_res:
        case _ if comm_or_res == RESIDENTIAL:
            typologies_filter = RESIDENTIAL_TYPOLOGIES
        case _ if comm_or_res == COMMERCIAL:
            typologies_filter = NON_RESIDENTIAL_TYPOLOGIES
        case _ if comm_or_res == RES_AND_COMM:
            typologies_filter = None
        case _:
            raise KeyError(f"{comm_or_res} is not a valid usage!")

    ratios: dict[int, float] = {}

    for year in range(scenario.start, scenario.end + 1, scenario.step_size):
        total_area = building_stock.get_total_area(year, typologies_filter)

        match use:
            case _ if use == DEMOLITION:
                activity_area = building_stock.get_demolition_area(year, typologies_filter)
            case _ if use == REFURBISHMENT:
                activity_area = building_stock.get_refurbishment_area(year, typologies_filter)
            case _ if use == CONSTRUCTION:
                activity_area = building_stock.get_construction_area(year, typologies_filter)
            case _:
                raise KeyError(f"{use} is not a valid use key!")

        ratios[year] = activity_area / total_area * 100

        if is_generation_run:
            break

    result_data: dict[str, str | float] = {
        "Variable": get_name_variable(comm_or_res, use),
        "Unit": "%",
    } | {f"{year}": ratio for year, ratio in ratios.items()}

    return pl.DataFrame(result_data, orient="row")
