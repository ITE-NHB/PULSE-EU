"""
building.py
-----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file provides a building object that can be acted upon.
"""

from __future__ import annotations

from pulse.support.archetype_data import (
    get_building_capacity,
    get_usage_distribution,
    get_useful_floor_area,
    get_weibull,
)
from pulse.support.defines import POST_WAR, PRE_WAR
from pulse.support.distributions import distribute_fully

from .scenario import Scenario
from .import_past_buildings import import_building
from .code import (
    code_is_demolishable,
    code_is_exb,
    code_is_new,
    code_is_refurbishable,
    code_is_ref,
    code_is_repurposable,
    get_epoch_start_from_code,
    get_epoch_end_from_code,
    get_typology_from_code,
    get_use_from_code,
    code_is_residential,
)


class Building:
    """A class for a Building."""

    archetype_name: str
    """The archetype name, also called code, of the building."""
    typology: str
    """The typology of the building."""

    scenario: Scenario
    """The scenario data."""

    # Useful static data
    is_residential: bool
    """Whether the building is residential."""
    can_be_demolished: bool
    """Whether the building can be demolished."""
    can_be_refurbished: bool
    """Whether the building can be refurbished."""
    can_be_repurposed: bool
    """Whether the building can be repurposed to REF 3 type 3."""

    is_exb: bool
    """Whether the building is of type EXB."""
    is_ref: bool
    """Whether the building is of type REF."""
    is_new: bool
    """Whether the building is of type NEW."""

    epoch_start: int
    """The start of the epoch defined in the code."""
    epoch_end: int
    """The end of the epoch defined in the code."""
    useful_floor_area_per_building: float
    """The area per building."""
    base_usage_distribution: tuple[float, float, float]
    """The distribution of the usage of the building."""
    base_capacity: float
    """The amount of people the building can house."""
    demolition_stats: list[float]
    """The weibull demolition statistics for the building."""
    renovation_factor: float
    """The renovation factor for the building from RENOVATION_DISTRIBUTIONS[typology][epoch]."""

    building_counts: list[int]
    """The amount of buildings for each year from 1850 to 2050"""
    building_distribution: list[int]
    """When the remaining buildings were built. First index is self.epoch_start, last is self.epoch_end"""
    demolition_counts: list[int]
    """How many buildings were demolished each year from 2020."""
    refurbishment_counts: list[int]
    """How many buildings were refurbished each year from 2020."""
    construction_counts: list[int]
    """How many buildings were constructed each year from 2020."""

    cumulative_demolitions: list[float]
    """The cumulative demolitions for each year from self.epoch_start to self.epoch_end."""

    base_buildings: list[str]
    """The base buildings for the building. Only set for refurbished buildings."""
    light_ref: Building | None = None
    """The light refurbishment for the building."""
    medium_ref: Building | None = None
    """The medium refurbishment for the building."""
    deep_ref: Building | None = None
    """The deep refurbishment for the building."""

    def __init__(
        self, archetype_name: str, scenario: Scenario, renovation_distributions: dict[str, dict[str, float]]
    ) -> None:
        self.archetype_name = archetype_name
        self.scenario = scenario

        self.typology = get_typology_from_code(archetype_name)

        # Useful static data
        self.is_residential = code_is_residential(archetype_name)
        self.can_be_demolished = code_is_demolishable(archetype_name)
        self.can_be_refurbished = code_is_refurbishable(archetype_name)
        self.can_be_repurposed = code_is_repurposable(archetype_name)

        self.is_exb = code_is_exb(archetype_name)
        self.is_ref = code_is_ref(archetype_name)
        self.is_new = code_is_new(archetype_name)

        self.epoch_start = get_epoch_start_from_code(archetype_name)
        self.epoch_end = get_epoch_end_from_code(archetype_name)

        self.useful_floor_area_per_building = get_useful_floor_area(archetype_name)
        self.base_usage_distribution = get_usage_distribution(archetype_name)
        self.base_capacity = get_building_capacity(archetype_name) if self.is_residential else 0.0

        # Get weibull data
        assert (self.epoch_start >= 1945) == (
            self.epoch_end >= 1945
        ), "Current implementation does not support buildings spanning the war"

        self.demolition_stats = get_weibull()[get_use_from_code(archetype_name)][
            POST_WAR if self.epoch_start >= 1945 else PRE_WAR
        ]

        if self.can_be_refurbished:
            self.renovation_factor = renovation_distributions[self.typology][str(self.epoch_start)]

        # Importing past building data
        self.building_distribution = import_building(archetype_name)

        self.building_counts = [0] * (scenario.end - scenario.data_start + 1)

        summed = 0

        for year, count in enumerate(self.building_distribution, int(self.epoch_start)):
            summed += count
            self.building_counts[year - scenario.data_start] = summed

        for year in range(self.epoch_end + 1, scenario.data_end + 1):
            self.building_counts[year - scenario.data_start] = summed

        self.demolition_counts = [0] * (self.scenario.end - self.scenario.start + 1)
        self.refurbishment_counts = [0] * (self.scenario.end - self.scenario.start + 1)
        self.construction_counts = [0] * (self.scenario.end - self.scenario.start + 1)

        self.cumulative_demolitions = [0.0] * (self.epoch_end - self.epoch_start + 1)

    def advance_year(self, year: int) -> None:
        """Advances the year of the building and updates the data."""
        idx = year - self.scenario.data_start

        self.building_counts[idx] = self.building_counts[idx - 1]

    def get_share_of_usage(self, year: int, artificial_usage: tuple[float, float, float] = (0, 0, 0)) -> float:
        """Returns the usage distribution for the building based on the scenario."""
        distribution = self.base_usage_distribution

        use_of_empty = self.scenario.use_of_empty[year]
        use_of_empty = (
            artificial_usage[0] or use_of_empty[0],
            artificial_usage[1] or use_of_empty[1],
            artificial_usage[2] or use_of_empty[2],
        )

        assert all(0 <= x <= 1 for x in use_of_empty), "Usage distribution must be between 0 and 1."

        if self.is_residential:
            return distribution[0] + distribution[1] * use_of_empty[0] + distribution[2] * use_of_empty[1]

        return distribution[0] + distribution[2] * use_of_empty[2]

    def get_capacity(self, year: int) -> int:
        """Gets the amount of people this building can house based on the scenario."""
        if not self.is_residential:
            return 0

        num_buildings = self.building_counts[year - self.scenario.data_start]
        capacity = self.base_capacity
        multiplier = self.get_share_of_usage(year)

        return int(num_buildings * capacity * multiplier + 0.5)

    def get_total_floor_area(self, year: int) -> float:
        """Gets the total useful floor area of all buildings of this archetype in a given year."""
        return self.building_counts[year - self.scenario.data_start] * self.useful_floor_area_per_building

    def get_used_floor_area(self, year: int) -> float:
        """Gets the total occupied useful floor area of all buildings of this archetype in a given year."""
        num_buildings = self.building_counts[year - self.scenario.data_start]
        floor_area = self.useful_floor_area_per_building
        multiplier = self.get_share_of_usage(year)

        return num_buildings * floor_area * multiplier

    def add_buildings(self, year: int, amount: int) -> float:
        """Adds buildings to the building stock and returns their area."""
        assert (
            self.scenario.start <= year <= self.scenario.end
        ), "Can only construct buildings within the scenario timeframe."

        idx = year - self.scenario.data_start
        self.building_counts[idx] += amount

        idx = year - self.epoch_start
        self.building_distribution[idx] += amount

        idx = year - self.scenario.start
        self.construction_counts[idx] += amount

        return amount * self.useful_floor_area_per_building

    def refurbish(self, year: int, amount: int, depth: str) -> None:
        """Refurbishes the building to the specified depth."""
        assert amount > 0, "Can only refurbish positive amounts of buildings."
        assert amount <= self.building_counts[year - self.scenario.data_start], "Not enough buildings to refurbish."

        match (depth):
            case "Light":
                ref_building = self.light_ref
            case "Medium":
                ref_building = self.medium_ref
            case "Deep":
                ref_building = self.deep_ref
            case _:
                raise ValueError("Invalid refurbishment depth.")

        assert ref_building is not None, "Building cannot be refurbished to that depth."

        building_distribution = dict(zip(range(self.epoch_start, self.epoch_end + 1), self.building_distribution))
        refurbishments_distribution = distribute_fully(amount, building_distribution, normalize=True)

        idx = year - self.scenario.data_start
        self.building_counts[idx] -= amount
        ref_building.building_counts[idx] += amount

        idx = year - self.scenario.start
        ref_building.construction_counts[idx] += amount
        self.refurbishment_counts[idx] += amount

        for dist_year, year_amount in refurbishments_distribution.items():
            idx = dist_year - self.epoch_start
            self.building_distribution[idx] -= year_amount
            ref_building.building_distribution[idx] += year_amount

    def refurbish_to_building_in_year(self, year: int, amount: int, ref_year: int, ref_building: Building) -> None:
        """Refurbished an amount of buildings in a single year to the specified building."""
        assert self.epoch_start <= ref_year <= self.epoch_end, "Can only ref buildings within the building epoch."
        assert amount > 0, "Can only refurbish positive amounts of buildings."
        assert amount <= self.building_counts[year - self.scenario.data_start], "Not enough buildings to refurbish."

        idx = year - self.scenario.data_start
        self.building_counts[idx] -= amount
        ref_building.building_counts[idx] += amount

        idx = year - self.scenario.start
        ref_building.construction_counts[idx] += amount
        self.refurbishment_counts[idx] += amount

        idx = ref_year - self.epoch_start
        self.building_distribution[idx] -= amount
        ref_building.building_distribution[idx] += amount

    def validate(self, year: int) -> None:
        """Validates the data integrity of the building."""
        data_start_idx = year - self.scenario.data_start
        assert sum(self.building_distribution) == self.building_counts[data_start_idx]

        scenario_start_idx = year - self.scenario.start
        assert self.building_counts[data_start_idx] == (
            self.building_counts[data_start_idx - 1]
            - self.demolition_counts[scenario_start_idx]
            - self.refurbishment_counts[scenario_start_idx]
            + self.construction_counts[scenario_start_idx]
        )
