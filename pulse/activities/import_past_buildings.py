"""
import_past_buildings.py
------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains functions for importing the existing building stock.
"""

from math import e, exp

from pulse.support.archetype_data import (
    get_buildingstock_dataframe,
    get_last_building_epoch_start,
    get_non_residential_epochs,
    get_residential_epochs,
    get_std_population_statistics,
)

from pulse.support.distributions import distribute_fully
from .code import code_is_residential, get_epoch_start_from_code, get_epoch_end_from_code

MEETING_POINT = 2000


def get_population_changes(population_development: dict[int, int]) -> dict[int, int]:
    """This function removes the negative values in the list, since this part of the LCA is not accounting for
    demolition. It is assumed, that a very small amount of buildings are being build, despite the
    population shrinking in a given year."""
    output: dict[int, int] = {}
    old_population = list(population_development.values())[0]

    for year, population in population_development.items():
        population_change = population - old_population
        old_population = population

        # Clean up the data using the exponential regression formula under the meeting point
        if population_change > MEETING_POINT:
            output[year] = population_change
        else:
            output[year] = max(round((MEETING_POINT / e) * exp(population_change / MEETING_POINT)), 1)

    return output


def get_age_range(
    population_changes: dict[int, int], residential: list[tuple[int, int]], non_residential: list[tuple[int, int]]
) -> tuple[dict[int, dict[int, float]], dict[int, dict[int, float]]]:
    """
    This function creates a number of percentages based on population increase and specified ranges.

    It returns a tuple where the first element is the residential data and the second is the non-residential data.
    The ductionaries map from the epoch start to a dictionary of years to percentages.
    """
    residential_d: dict[int, dict[int, float]] = {}
    non_residential_d: dict[int, dict[int, float]] = {}

    for epoch_start, epoch_end in residential:
        residential_d[epoch_start] = {}

        total = 0
        for year in range(epoch_start, epoch_end + 1):
            total += population_changes[year]

        for year in range(epoch_start, epoch_end + 1):
            residential_d[epoch_start][year] = population_changes[year] / total

        if epoch_start == get_last_building_epoch_start():
            count = len(residential_d[epoch_start])
            residential_d[epoch_start] = {year: 1 / count for year in residential_d[epoch_start].keys()}

    for epoch_start, epoch_end in non_residential:
        non_residential_d[epoch_start] = {}

        total = 0
        for year in range(epoch_start, epoch_end + 1):
            total += population_changes[year]

        for year in range(epoch_start, epoch_end + 1):
            non_residential_d[epoch_start][year] = population_changes[year] / total

        if epoch_start == get_last_building_epoch_start():
            count = len(non_residential_d[epoch_start])
            non_residential_d[epoch_start] = {year: 1 / count for year in non_residential_d[epoch_start].keys()}

    return residential_d, non_residential_d


def calculate_construction_statistics() -> tuple[dict[int, dict[int, float]], dict[int, dict[int, float]]]:
    """
    This function calculates the historic statistics as a tuple, where the 1st is non-res, the 2nd is res.

    It returns a tuple where the first element is the residential data and the second is the non-residential data.
    The ductionaries map from the epoch start to a dictionary of years to percentages.
    """
    # Getting the age ranges from the data
    population_development = get_std_population_statistics()

    start = list(population_development.keys())[0]
    end = list(population_development.keys())[-1]
    assert all(year in population_development for year in range(start, end + 1))

    pop_change = get_population_changes(population_development)

    res, non_res = get_age_range(
        pop_change,
        residential=get_residential_epochs(),
        non_residential=get_non_residential_epochs(),
    )

    assert all(
        all(change >= 0 for change in yearly_data.values()) for yearly_data in res.values()
    ), "The residential population changes are negative at points!"

    assert all(
        all(change >= 0 for change in yearly_data.values()) for yearly_data in non_res.values()
    ), "The non-residential population changes are negative at points!"

    return res, non_res


class ConstructionStats:
    """This class calculates the population statistics once and stores them afterwards."""

    _res_pop_data: dict[int, dict[int, float]] = {}
    _non_res_pop_data: dict[int, dict[int, float]] = {}
    _building_data: dict[str, float] = {}

    def __init__(self) -> None:
        assert (
            not ConstructionStats._res_pop_data
            and not ConstructionStats._non_res_pop_data
            and not ConstructionStats._building_data
        ), "PopulationStats should not be initialized more than once!"

        ConstructionStats._res_pop_data, ConstructionStats._non_res_pop_data = calculate_construction_statistics()

        b_counts = get_buildingstock_dataframe().select("archetype_name", "number of buildings")
        ConstructionStats._building_data = dict(zip(b_counts.to_series(0), b_counts.to_series(1)))

    @staticmethod
    def get_yearly_changes(residential: bool, epoch_start: int) -> dict[int, float]:
        """Gets the yearly changes in the population."""
        if not ConstructionStats._res_pop_data:
            ConstructionStats()

        if residential:
            return ConstructionStats._res_pop_data[epoch_start]

        return ConstructionStats._non_res_pop_data[epoch_start]

    @staticmethod
    def building_count(archetype_name: str) -> float:
        """Gets the building count for a given archetype name."""
        if not ConstructionStats._building_data:
            ConstructionStats()

        data = ConstructionStats._building_data

        return data[archetype_name]


def import_building(archetype_name: str) -> list[int]:
    """This function imports the construction data for a building."""

    is_residential = code_is_residential(archetype_name)

    epoch_start = get_epoch_start_from_code(archetype_name)
    epoch_end = get_epoch_end_from_code(archetype_name)

    number_of_buildings = round(ConstructionStats.building_count(archetype_name))

    if number_of_buildings != 0:
        age_range_percentages = ConstructionStats.get_yearly_changes(is_residential, epoch_start)

        distributed = distribute_fully(number_of_buildings, age_range_percentages)
    else:
        distributed = {}

    construction_data: list[int] = []

    for year in range(epoch_start, epoch_end + 1):
        count = 0 if year not in distributed else distributed[year]

        construction_data.append(count)

    return construction_data
