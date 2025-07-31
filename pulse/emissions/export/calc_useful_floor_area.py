"""
calc_useful_floor_area.py
-------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the function to calculate the useful floor area for different building types.
"""

import polars as pl

from pulse.activities import BuildingStock, Scenario
from pulse.support.defines import RESIDENTIAL_TYPOLOGIES, TYPOLOGIES, TYPOLOGY_SHORT_TO_LONG

from .defines import (
    COMMERCIAL,
    RES_AND_COMM,
    RESIDENTIAL,
    STOCK,
    DEMOLISHED,
    NEW,
    RENOVATED,
)


def get_name_variable(typology: str, use: str) -> str:
    """This function gets the name variable for the pyam file."""

    if typology == RES_AND_COMM:
        comm_or_res = RES_AND_COMM
        typo_string = "Total"
    else:
        comm_or_res = RESIDENTIAL if typology in RESIDENTIAL_TYPOLOGIES else COMMERCIAL
        typo_string = TYPOLOGY_SHORT_TO_LONG[typology]

    return f"Energy Service|{comm_or_res}|{typo_string}|Floor space|{use}"


def calculate_useful_floor_area(
    scenario: Scenario, building_stock: BuildingStock, use: str, is_generation_run: bool
) -> pl.DataFrame:
    """This function calculates the total useful floor area of the chosen type of buildings."""
    match use:
        case _ if use == STOCK:
            area_function = building_stock.get_total_area
        case _ if use == DEMOLISHED:
            area_function = building_stock.get_demolition_area
        case _ if use == RENOVATED:
            area_function = building_stock.get_refurbishment_area
        case _ if use == NEW:
            area_function = building_stock.get_construction_area
        case _:
            raise KeyError(f"{use} is not a valid use key!")

    result_data: list[dict[str, str | float]] = []

    total: dict[int, float] = {}

    # Calculate the useful floor area by multiplying reference floor area by number of buildings
    for typology in TYPOLOGIES:
        new_data: dict[str, str | float] = {
            "Variable": get_name_variable(typology, use),
            "Unit": "mn m²",
        }

        for year in range(scenario.start, scenario.end + 1, scenario.step_size):
            area = area_function(year, [typology]) / 1_000_000
            new_data |= {f"{year}": area}

            total.setdefault(year, 0)
            total[year] += area

            if is_generation_run:
                break

        result_data.append(new_data)

    new_data = {
        "Variable": get_name_variable(RES_AND_COMM, use),
        "Unit": "mn m²",
    }

    for year in range(scenario.start, scenario.end + 1, scenario.step_size):
        new_data[str(year)] = total[year]

        if is_generation_run:
            break

    result_data.append(new_data)

    return pl.concat([pl.DataFrame(data, orient="row") for data in result_data])
