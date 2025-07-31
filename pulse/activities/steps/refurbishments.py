"""
refurbishments.py
-----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file holds the refurbishments function for the building stock.
"""

import math

from pulse.support.defines import DEEP, LIGHT, MEDIUM, NON_RESIDENTIAL_TYPOLOGIES, RESIDENTIAL_TYPOLOGIES, TYPOLOGIES
from pulse.support.distributions import distribute_fully_capped
from pulse.support.logging import log_warning

from pulse.activities.building import Building
from pulse.activities.scenario import Scenario


def get_refurbishments_totals_sqm(
    year: int, buildings: dict[str, Building], scenario: Scenario
) -> dict[str, dict[str, int]]:
    """This function gets the refurbishment counts for light, medium, deep for each typology."""
    res_ref_percentage = scenario.residential_ref[year]
    non_res_ref_percentage = scenario.non_residential_ref[year]

    # This dict contains the total amount of buildings by typology before the last epoch
    typology_totals: dict[str, float] = {typology: 0 for typology in TYPOLOGIES}

    # Use this to have it always act on 2019 (data end)
    # idx = scenario.data_end - scenario.data_start

    # Use this to have it always act on the current year
    idx = year - scenario.data_start

    for building in buildings.values():
        amount = building.useful_floor_area_per_building * building.building_counts[idx]
        typology_totals[building.typology] += amount

    # This dict contains how many refurbishments of each type (light, medium, deep) get done for each typology in total
    refurbishment_totals: dict[str, dict[str, int]] = {}

    for typology in RESIDENTIAL_TYPOLOGIES:
        total = typology_totals[typology]

        refurbishment_totals[typology] = {
            LIGHT: int(total * res_ref_percentage[LIGHT] + 0.5),
            MEDIUM: int(total * res_ref_percentage[MEDIUM] + 0.5),
            DEEP: int(total * res_ref_percentage[DEEP] + 0.5),
        }

    for typology in NON_RESIDENTIAL_TYPOLOGIES:
        total = typology_totals[typology]

        refurbishment_totals[typology] = {
            LIGHT: int(total * non_res_ref_percentage[LIGHT] + 0.5),
            MEDIUM: int(total * non_res_ref_percentage[MEDIUM] + 0.5),
            DEEP: int(total * non_res_ref_percentage[DEEP] + 0.5),
        }

    return refurbishment_totals


def distribute_refurbishments_sqm(
    year: int, ref_area: int, relevant_buildings: dict[str, Building], renovations: dict[str, int]
) -> tuple[dict[str, int], int]:
    """Distributes the refurbishments across the supplied buildings."""
    if len(relevant_buildings) == 0 or ref_area <= 0:
        return {}, ref_area

    epoch_area_totals: dict[int, float] = {}
    for building in relevant_buildings.values():
        epoch_area_totals.setdefault(building.epoch_start, 0)
        area = building.building_counts[year - building.scenario.data_start] * building.useful_floor_area_per_building

        epoch_area_totals[building.epoch_start] += area

    areas: dict[str, int] = {}
    distribution: dict[str, float] = {}

    for code, building in relevant_buildings.items():
        count = building.building_counts[year - building.scenario.data_start]
        if count == 0:
            continue

        useful_floor_area = building.useful_floor_area_per_building

        area = count * useful_floor_area

        # Round down without + 0.5 at the end to make sure we never try to ref more than the total area
        areas[code] = int(area - (renovations.get(code, 0) * useful_floor_area))
        distribution[code] = area / epoch_area_totals[building.epoch_start] * building.renovation_factor

    # These contain the weights of the refurbishment types per building archetype
    ref_distributed, _, overflow = distribute_fully_capped(
        count=ref_area,
        distribution=distribution,
        caps=areas,
    )

    extra_overflow = 0.0

    refurbishments: dict[str, int] = {}

    for code, refurbished_area in ref_distributed.items():
        building = relevant_buildings[code]

        useful_floor_area = building.useful_floor_area_per_building

        ref_count = int(refurbished_area / useful_floor_area + 0.5)
        extra_overflow += refurbished_area - (ref_count * useful_floor_area)

        assert code not in refurbishments
        refurbishments[code] = ref_count
        renovations[code] = renovations.get(code, 0) + ref_count

    return refurbishments, overflow + int(extra_overflow + 0.5)


def do_refurbishments(
    year: int,
    buildings: dict[str, Building],
    scenario: Scenario,
    refurbishable_buildings: dict[str, dict[str, dict[str, Building]]],
) -> None:
    """
    Renovates the buildings in the building stock based on the provided Scenario.
    """
    # This dict contains how many refurbishments of each type (light, medium, deep) get done for each typology in total
    refurbishment_totals = get_refurbishments_totals_sqm(year, buildings, scenario)

    for typology in TYPOLOGIES:
        typo_ref_area = refurbishment_totals[typology]
        typology_buildings = refurbishable_buildings[typology]

        renovation_counts: dict[str, int] = {}

        d_relevant = typology_buildings[DEEP]
        d_refs, d_overflow = distribute_refurbishments_sqm(year, typo_ref_area[DEEP], d_relevant, renovation_counts)
        typo_ref_area[MEDIUM] += d_overflow

        m_relevant = typology_buildings[MEDIUM]
        m_refs, m_overflow = distribute_refurbishments_sqm(year, typo_ref_area[MEDIUM], m_relevant, renovation_counts)
        typo_ref_area[LIGHT] += m_overflow

        l_relevant = typology_buildings[LIGHT]
        l_refs, l_overflow = distribute_refurbishments_sqm(year, typo_ref_area[LIGHT], l_relevant, renovation_counts)

        for code, amount in d_refs.items():
            if amount > 0:
                d_relevant[code].refurbish(year, amount, DEEP)

        for code, amount in m_refs.items():
            if amount > 0:
                m_relevant[code].refurbish(year, amount, MEDIUM)

        for code, amount in l_refs.items():
            if amount > 0:
                l_relevant[code].refurbish(year, amount, LIGHT)

        # if CHECK_DATA_INTEGRITY:
        #     assert all(value >= 0 for value in occurrence_counts.values())

        #     assert (
        #         all(value >= 0 for value in l_ref_distributed.values())
        #         and all(value >= 0 for value in m_ref_distributed.values())
        #         and all(value >= 0 for value in ref_distributed.values())
        #     )

        if d_overflow or m_overflow or l_overflow:
            log_warning(
                f"{year} {typology} Overflow: Deep: {d_overflow}/{typo_ref_area[DEEP]}, "
                f"Medium: {m_overflow}/{typo_ref_area[MEDIUM]}, Light: {l_overflow}/{typo_ref_area[LIGHT]}"
            )

    refurb_use_of_emptys(year, buildings, scenario)


def refurb_use_of_emptys(year: int, buildings: dict[str, Building], scenario: Scenario) -> None:
    """Applies the refurbishment to the use of emptys."""
    use_of_empty = scenario.use_of_empty[year]

    if sum(use_of_empty) == 0:
        return

    prev_use_of_empty = scenario.use_of_empty[year - 1]

    if use_of_empty == prev_use_of_empty:
        return

    year_ind = year - scenario.data_start

    for building in buildings.values():
        # Skip excluded codes.
        if not building.can_be_refurbished or building.deep_ref is None:
            continue

        distribution = building.base_usage_distribution

        deep_this_year: int = math.ceil(
            (distribution[1] * use_of_empty[0] + distribution[2] * use_of_empty[1]) * building.building_counts[year_ind]
        )
        deep_prev_year: int = math.ceil(
            (distribution[1] * prev_use_of_empty[0] + distribution[2] * prev_use_of_empty[1])
            * building.building_counts[year_ind - 1]
        )

        diff = deep_this_year - deep_prev_year

        if diff <= 0:
            continue

        building.refurbish(year, diff, DEEP)
