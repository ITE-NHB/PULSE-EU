"""
demolitions.py
--------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file holds the demolitions function for the building stock.
"""

import math

from pulse.activities.building import Building


def do_demolitions(year: int, buildings: dict[str, Building]) -> tuple[float, float]:
    """
    Demolishes the buildings in the building stock based on the provided Scenario and returns the demolished area.
    """
    demolished_area_res = 0.0
    demolished_area_non_res = 0.0

    for building in buildings.values():
        if not building.can_be_demolished:
            continue

        demolitions = [0] * (building.epoch_end - building.epoch_start + 1)

        for building_year in range(building.epoch_start, building.epoch_end + 1):
            idx = building_year - building.epoch_start

            building_count = building.building_distribution[idx]

            if building_count == 0:
                continue

            age = year - building_year

            # Get the amount of building to be demolished in that specific year (cumulative)
            demo_amount = building_count * building.demolition_stats[age] + building.cumulative_demolitions[idx]

            assert demo_amount >= 0

            building.cumulative_demolitions[idx] = demo_amount % 1

            demo_amount = int(demo_amount)

            if demo_amount == 0:
                continue

            demolitions[idx] = demo_amount

        repurpose_demolished_buildings(year, building, demolitions)

        demolished_buildings = sum(demolitions)

        building.building_counts[year - building.scenario.data_start] -= demolished_buildings
        building.demolition_counts[year - building.scenario.start] += demolished_buildings

        for idx, amount in enumerate(demolitions):
            building.building_distribution[idx] -= amount

        demolished_area = demolished_buildings * building.useful_floor_area_per_building

        if building.is_residential:
            demolished_area_res += demolished_area
        else:
            demolished_area_non_res += demolished_area

    return demolished_area_res, demolished_area_non_res


def repurpose_demolished_buildings(year: int, building: Building, demolitions: list[int]) -> None:
    """ "
    Calculates the repurposing of demolished buildings for a given year.
    """
    repurpose_rate = building.scenario.repurpose_demo[year]

    if repurpose_rate == 0:
        return

    if (not building.can_be_repurposed) or (building.deep_ref is None) or sum(demolitions) == 0:
        return

    for dem_year, amount in enumerate(demolitions, building.epoch_start):
        if amount == 0:
            continue

        amt_to_ref = int(math.ceil(amount * repurpose_rate))
        assert amt_to_ref >= 0

        if amt_to_ref == 0:
            continue

        demolitions[dem_year - building.epoch_start] -= amt_to_ref
        building.refurbish_to_building_in_year(year, amt_to_ref, dem_year, building.deep_ref)
