"""
constructions.py
----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file holds the constructions function for the building stock.
"""

import math

from pulse.support.file_interaction import load_minimum_construction_rate, load_virtual_population
from pulse.support.logging import log_info
from pulse.support.distributions import distribute_fully
from pulse.support.archetype_data import get_heating_system_distribution, get_initial_std_avg_floor_area
from pulse.support.defines import (
    FIXED_CONSTRUCTION_AREA_MULTIPLIER,
    NEW_CON_EPOCH,
    RESIDENTIAL_TYPOLOGIES,
    NON_RESIDENTIAL_TYPOLOGIES,
    TYPOLOGIES,
)

from pulse.activities.building import Building
from pulse.activities.scenario import Scenario
from pulse.activities.construction_statistics import ConstructionStatistics


def distribute_constructions(
    amount: int, typology: str, country_code: str, typology_stats: ConstructionStatistics
) -> dict[str, int]:
    """Distributes the constructions onto the typology."""
    code_probabilities = {f"{country_code}-{typology}-{NEW_CON_EPOCH[0]}-{NEW_CON_EPOCH[1]}-NEW": 1.0}

    # Construction level distribution
    distr = typology_stats.construction[typology]

    if sum(distr.values()) == 0:
        distr["1"] = 1

    assert round(sum(distr.values()), 10) == 1, "The distribution should always add to 1!"

    code_probabilities = {
        f"{code}-0{type_i}": probability * type_probability
        for code, probability in code_probabilities.items()
        for type_i, type_probability in distr.items()
    }

    # Energy performance level distribution
    distr = typology_stats.energy_performance[typology]

    if sum(distr.values()) == 0:
        distr["1"] = 1

    assert round(sum(distr.values()), 10) == 1, "The distribution should always add to 1!"

    code_probabilities = {
        f"{code}-0{type_i}": probability * type_probability
        for code, probability in code_probabilities.items()
        for type_i, type_probability in distr.items()
    }

    # Heating system level distribution
    code_probabilities = {
        f"{code}-{type_i}": probability * type_probability
        for code, probability in code_probabilities.items()
        for type_i, type_probability in get_heating_system_distribution(code).items()
    }

    return distribute_fully(amount, code_probabilities)


def get_housing_demand(
    year: int,
    buildings: dict[str, Building],
    scenario: Scenario,
    construction_stats: ConstructionStatistics,
) -> dict[str, int]:
    """
    Gets the construction area for a given year and scenario.
    If residential is True, the function will return the population increase per typology.
    If residential is False, the function will return the square meter increase per typology.
    """
    floor_area_increase_factor = scenario.floor_area[year] / get_initial_std_avg_floor_area()

    pop_capacity = (
        sum(building.get_capacity(year) for building in buildings.values() if building.is_residential)
        / floor_area_increase_factor
    )

    pop_demand = scenario.population[year]
    pop_increase = int(pop_demand - pop_capacity + 0.5)
    pop_increase = max(pop_increase, 0)

    # This distributes the new Population among the typologies
    return distribute_fully(pop_increase, construction_stats.typology_distribution)


def get_non_residential_demand(
    year: int,
    buildings: dict[str, Building],
    scenario: Scenario,
    construction_stats: ConstructionStatistics,
) -> dict[str, int]:
    """Gets the non-residential construction area for a given year and scenario."""
    population = load_virtual_population(scenario.country_code)

    sqm_increase: dict[str, int] = {}

    for typology in NON_RESIDENTIAL_TYPOLOGIES:
        current_sqm = sum(
            building.get_used_floor_area(year)
            for building in buildings.values()
            if not building.is_residential and building.typology == typology
        )

        sqm_demand = population[year] * construction_stats.sqm_per_person[typology]
        sqm_increase[typology] = int(sqm_demand - current_sqm + 0.5)
        sqm_increase[typology] = max(sqm_increase[typology], 0)

    log_info(
        f"{year} Loaded virtual population ({population[year]}) "
        + f"with an area increase of {sum(sqm_increase.values())}m²"
    )

    return sqm_increase


def get_fixed_area_demand(all_buildings: dict[str, Building], scenario: Scenario) -> dict[str, int]:
    """Gets the fixed area demand for a given year and scenario."""
    con_rate = load_minimum_construction_rate(scenario.country_code, residential=False)
    last_data_year_area: dict[str, float] = {typology: 0.0 for typology in NON_RESIDENTIAL_TYPOLOGIES}

    for building in all_buildings.values():
        if building.is_residential:
            continue

        last_data_year_area[building.typology] += building.get_used_floor_area(scenario.data_end)

    sqm_increase = {
        typology: int(con_rate * area * FIXED_CONSTRUCTION_AREA_MULTIPLIER)
        for typology, area in last_data_year_area.items()
    }

    return sqm_increase


def do_constructions(
    year: int,
    all_buildings: dict[str, Building],
    scenario: Scenario,
    ref_construction_stats: ConstructionStatistics,
    generation_run: bool = False,
) -> tuple[float, float]:
    """Calculates the constructions of each archetype for a given year and scenario. Returns the constructed area."""
    constructed_area_res = 0.0
    constructed_area_non_res = 0.0

    construction_stats = adapt_construction_stats(ref_construction_stats, scenario, year)

    pop_increase_dict = get_housing_demand(
        year=year,
        buildings=all_buildings,
        scenario=scenario,
        construction_stats=construction_stats,
    )

    floor_area_increase_factor = scenario.floor_area[year] / get_initial_std_avg_floor_area()

    for typology in RESIDENTIAL_TYPOLOGIES:
        pop_increase = pop_increase_dict[typology]

        if pop_increase == 0:
            continue

        construction_distribution = distribute_constructions(
            pop_increase, typology, scenario.country_code, construction_stats
        )

        for code, pop in construction_distribution.items():
            building = all_buildings[code]

            share_of_used = building.get_share_of_usage(year)
            capacity = building.base_capacity / floor_area_increase_factor

            constructed_area_res += building.add_buildings(year, math.ceil(pop / capacity / share_of_used))

    if not generation_run:
        sqm_increase_dict = get_non_residential_demand(
            year=year,
            buildings=all_buildings,
            scenario=scenario,
            construction_stats=construction_stats,
        )
    else:
        sqm_increase_dict = get_fixed_area_demand(all_buildings, scenario)

    for typology in NON_RESIDENTIAL_TYPOLOGIES:
        sqm_increase = sqm_increase_dict[typology]

        if sqm_increase == 0:
            continue

        construction_distribution = distribute_constructions(
            sqm_increase, typology, scenario.country_code, construction_stats
        )

        for code, sqm in construction_distribution.items():
            building = all_buildings[code]

            share_of_used = building.get_share_of_usage(year)
            constructed_area_non_res += building.add_buildings(
                year, math.ceil(sqm / building.useful_floor_area_per_building / share_of_used)
            )

    return constructed_area_res, constructed_area_non_res


def adapt_construction_stats(
    ref_con_stats: ConstructionStatistics, scenario: Scenario, year: int
) -> ConstructionStatistics:
    """This function solves the impact of scenario specifications."""

    def adapt_stat(ref: dict[str, float], new: dict[str, float]) -> dict[str, float]:
        """This function adapts statistics to a changed input."""
        # Parameter checking start
        assert (
            round(sum(ref.values()), 5) == 1
        ), "ERROR during adaption of statistics - The reference values need to add to 1!"

        assert all(
            (key in RESIDENTIAL_TYPOLOGIES) or (len(key) in (1, 2) and 0 <= int(key) <= 99) for key in new.keys()
        ), f"ERROR during adaption of statistics - Invalid key: {list(new.keys())}"

        assert len(set(new).difference(ref)) == 0, (
            "ERROR during adaption of statistics - Keys do not match: "
            + f"ref: {list(ref.keys())}, new: {list(new.keys())}"
        )

        if all(value is None for value in new.values()):
            return ref

        summed = sum(val for val in new.values() if val is not None)
        assert summed <= 1, f"ERROR during adaption of statistics - The sum is above 1: {summed}"
        # Parameter checking end

        if len(new) == len(ref) and all(item is not None for item in new.values()):
            return new

        up = down = 1.0
        return_: dict[str, float] = {}

        for key, a in ref.items():
            if new.get(key, None) is not None:
                up -= a
                down -= new[key]
                return_[key] = new[key]

        for key, a in ref.items():
            if key not in return_:
                return_[key] = a / up * down

        assert (
            round(sum(return_.values()), 4) == 1
        ), "ERROR during adaption of statistics - This Error should not happen - The return values don't add to 1!"

        return return_

    def multi_stat(ref: dict[str, float], multi: dict[str, float]) -> dict[str, float]:
        """This function multiplies statistics to a changed input."""

        assert len(set(multi).difference(ref)) == 0, (
            "ERROR during multiplication of statistics - Keys do not match: "
            + f"ref: {list(ref.keys())}, new: {list(multi.keys())}"
        )

        if all(value is None for value in multi.values()):
            return ref

        return {key: a * (1 + multi[key]) if key in multi and multi[key] else a for key, a in ref.items()}

    ref = ref_con_stats
    new = scenario.get_scenario_construction_statistics(year)

    typology_distribution = adapt_stat(ref.typology_distribution, new.typology_distribution)

    sqm_per_person = multi_stat(ref.sqm_per_person, new.sqm_per_person)

    heating: dict[str, dict[str, float]] = {}
    construction: dict[str, dict[str, float]] = {}
    energy_performance: dict[str, dict[str, float]] = {}

    for typology in TYPOLOGIES:
        heating[typology] = adapt_stat(ref.heating[typology], new.heating[typology])
        energy_performance[typology] = adapt_stat(ref.energy_performance[typology], new.energy_performance[typology])
        construction[typology] = adapt_stat(ref.construction[typology], new.construction[typology])

    return ConstructionStatistics(
        typology_distribution,
        construction,
        energy_performance,
        heating,
        sqm_per_person,
    )
