"""
population.py
-------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the function to calculate the population for different residential typologies.
"""

import polars as pl

from pulse.activities import BuildingStock, Scenario
from pulse.support.defines import RESIDENTIAL_TYPOLOGIES, TYPOLOGY_SHORT_TO_LONG

from .defines import RESIDENTIAL


def get_name_variable(typology: str) -> str:
    """This function gets the name variable for the pyam file."""
    typo_string = TYPOLOGY_SHORT_TO_LONG[typology]

    return f"Population|{RESIDENTIAL}|{typo_string}"


def calculate_population(scenario: Scenario, building_stock: BuildingStock, is_generation_run: bool) -> pl.DataFrame:
    """Calculates population using typology archetypes and the 'number of users' in the archetype stock data."""
    pop_distributions: dict[int, dict[str, float]] = {
        year: {typo: float(amt) for typo, amt in building_stock.get_capacity(year).items()}
        for year in range(scenario.start, scenario.end + 1, scenario.step_size)
    }

    pop_distributions = {
        year: {typology: value / sum(distribution.values()) for typology, value in distribution.items()}
        for year, distribution in pop_distributions.items()
    }

    result_data: list[dict[str, str | float]] = []

    for typology in RESIDENTIAL_TYPOLOGIES:
        new_data: dict[str, str | float] = {
            "Variable": get_name_variable(typology),
            "Unit": "million",
        }
        for year in range(scenario.start, scenario.end + 1, scenario.step_size):
            new_data[f"{year}"] = scenario.population[year] * pop_distributions[year][typology] / 1_000_000

            if is_generation_run:
                break

        result_data.append(new_data)

    new_data = {
        "Variable": f"Population|{RESIDENTIAL}|Total",
        "Unit": "million p",
    }

    for year in range(scenario.start, scenario.end + 1, scenario.step_size):
        new_data[f"{year}"] = scenario.population[year] / 1_000_000

        if is_generation_run:
            break

    result_data.append(new_data)

    return pl.concat([pl.DataFrame(data, orient="row") for data in result_data])
