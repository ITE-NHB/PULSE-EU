"""
building_stock.py
-----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file provides a building_stock class that holds the building stock data.
"""

import polars as pl

from pulse.support.ui import time_function
from pulse.support.archetype_data import get_buildingstock_codes, get_buildingstock_end_year
from pulse.support.defines import (
    CONSTRUCTION_TYPES,
    DEEP,
    FIXED_CONSTRUCTION_AREA_MULTIPLIER,
    LIGHT,
    MEDIUM,
    NON_RESIDENTIAL_TYPOLOGIES,
    RESIDENTIAL_TYPOLOGIES,
    TYPOLOGIES,
)
from pulse.support.file_interaction import (
    load_energy_performance_data,
    load_minimum_construction_rate,
    load_refurbishment_distribution,
    store_building_stock,
    store_construction,
    store_demolitions,
    store_refurbishments,
    store_virtual_population,
)
from pulse.support.logging import log_info, log_warning

from .code import get_construction_type_from_code, get_heating_type_from_code, get_refurbed_code
from .steps.demolitions import do_demolitions
from .steps.refurbishments import do_refurbishments
from .steps.constructions import do_constructions
from .construction_statistics import ConstructionStatistics
from .building import Building
from .scenario import Scenario


class BuildingStock:
    """A class for a Building Stock."""

    buildings: dict[str, Building]
    demolishable_buildings: dict[str, Building]
    refurbishable_buildings: dict[str, dict[str, dict[str, Building]]]
    """Typology -> Depth -> Code -> Building"""

    scenario: Scenario

    construction_stats: ConstructionStatistics

    collected_dataframes: tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame] | None = None
    """The dataframes for the building stock, demolitions, refurbishments and constructions."""

    def __init__(self, scenario: Scenario) -> None:
        """Creates a new BuildingStock."""
        self.scenario = scenario

        renovation_distributions = load_refurbishment_distribution()

        self.buildings = {
            archetype_name: Building(archetype_name, scenario, renovation_distributions)
            for archetype_name in get_buildingstock_codes()
        }

        self.demolishable_buildings = {}

        for code, building in self.buildings.items():
            if building.can_be_demolished:
                self.demolishable_buildings[code] = building

        exbs: dict[str, list[str]] = {}

        for code, building in self.buildings.items():
            if building.is_exb:
                exbs.setdefault(code[:-6], []).append(code)

        self.refurbishable_buildings = {typology: {LIGHT: {}, MEDIUM: {}, DEEP: {}} for typology in TYPOLOGIES}

        for code, exb_buildings in exbs.items():
            l_code = get_refurbed_code(code, "1")
            m_code = get_refurbed_code(code, "2")
            d_code = get_refurbed_code(code, "3")

            if l_code in self.buildings:
                self.buildings[l_code].base_buildings = exb_buildings.copy()

                for build in exb_buildings:
                    b_obj = self.buildings[build]
                    self.refurbishable_buildings[b_obj.typology][LIGHT][build] = b_obj

                    b_obj.light_ref = self.buildings[l_code]

            if m_code in self.buildings:
                self.buildings[m_code].base_buildings = exb_buildings.copy()

                for build in exb_buildings:
                    b_obj = self.buildings[build]
                    self.refurbishable_buildings[b_obj.typology][MEDIUM][build] = b_obj

                    self.buildings[build].medium_ref = self.buildings[m_code]

            if d_code in self.buildings:
                self.buildings[d_code].base_buildings = exb_buildings.copy()

                for build in exb_buildings:
                    b_obj = self.buildings[build]
                    self.refurbishable_buildings[b_obj.typology][DEEP][build] = b_obj

                    self.buildings[build].deep_ref = self.buildings[d_code]

            if l_code in self.buildings and d_code in self.buildings:
                self.buildings[l_code].deep_ref = self.buildings[d_code]

        self.construction_stats = self.__calculate_construction_stats()

    def __calculate_construction_stats(self) -> ConstructionStatistics:
        """Calculates the construction statistics for the building stock."""

        stock_end_year = get_buildingstock_end_year()

        def calculate_typology_distribution() -> dict[str, float]:
            # Only evaluate for the last time period, as older one's construction trends are not relevant
            typology_distribution_res: dict[str, float] = {typology: 0.0 for typology in RESIDENTIAL_TYPOLOGIES}

            for building in self.buildings.values():
                if not building.is_residential:
                    continue
                if building.epoch_end != stock_end_year:
                    continue

                typology_distribution_res[building.typology] += building.get_capacity(stock_end_year)

            total = sum(typology_distribution_res.values())

            # Convert absolute to percentage
            typology_distribution_res = {
                typology: typology_total / total for typology, typology_total in typology_distribution_res.items()
            }

            return typology_distribution_res

        def calculate_type_distributions() -> (
            tuple[dict[str, dict[str, float]], dict[str, dict[str, float]], dict[str, dict[str, float]]]
        ):
            # dict[typology, dict[type, percentage]]
            construction: dict[str, dict[str, float]] = {}
            heating: dict[str, dict[str, float]] = {}

            ep_data = load_energy_performance_data(self.scenario.country_code)
            energy_performance = {typology: ep_data.copy() for typology in TYPOLOGIES}

            # Calculate heating and construction distribution
            for typology in TYPOLOGIES:
                c_type_data = construction.setdefault(typology, {c_type: 0.0 for c_type in CONSTRUCTION_TYPES})
                h_type_data = heating.setdefault(typology, {})

                # This filters the building stock codes to only include relevant Buildings
                relevant = {
                    building.archetype_name: (
                        building.get_capacity(stock_end_year)
                        if building.is_residential
                        else building.get_used_floor_area(stock_end_year)
                    )
                    for building in self.buildings.values()
                    if building.typology == typology and building.epoch_end == stock_end_year
                }

                for code, capacity in relevant.items():
                    h_type = get_heating_type_from_code(code)
                    c_type = get_construction_type_from_code(code)

                    h_type_data.setdefault(h_type, 0)
                    c_type_data.setdefault(c_type, 0)

                    # Add to the total
                    h_type_data[h_type] += capacity
                    c_type_data[c_type] += capacity

                total_sum = sum(h_type_data.values())
                assert round(total_sum - sum(c_type_data.values()), 5) == 0, "Hea and Con total mismatch!"

                if total_sum <= 0:
                    log_warning(f'Hea and Con distribution for "{list(relevant.keys())[0][0:-9]}" evaluated to 0!')

                # Calculate the percentage distribution instead
                for c_type in h_type_data.keys():
                    h_type_data[c_type] /= total_sum or 1

                for c_type in c_type_data.keys():
                    c_type_data[c_type] /= total_sum or 1

                assert round(sum(h_type_data.values()), 5) in (0, 1)

            return construction, energy_performance, heating

        def get_sqm_per_person() -> dict[str, float]:
            population = self.scenario.population[stock_end_year]

            total_sqm: dict[str, float] = {typology: 0 for typology in NON_RESIDENTIAL_TYPOLOGIES}

            for building in self.buildings.values():
                if building.is_residential:
                    continue

                total_sqm[building.typology] += building.get_used_floor_area(stock_end_year)

            return {typology: sqm / population for typology, sqm in total_sqm.items()}

        return ConstructionStatistics(
            calculate_typology_distribution(),
            *calculate_type_distributions(),
            get_sqm_per_person(),
        )

    def get_floor_area_by_typology(
        self, year: int, grouped: bool = True, emissions: bool = False, log_empty: bool = False
    ) -> dict[str, float]:
        """This gets the total square meters occupied by a typology, possibly grouped by typology."""
        output: dict[str, float] = {}

        for code, building in self.buildings.items():
            if not emissions and building.is_residential:
                continue

            num_buildings = building.building_counts[year - building.scenario.data_start]

            floor_area = building.useful_floor_area_per_building
            multiplier = building.get_share_of_usage(year) if not emissions else 1.0

            building_sqm = num_buildings * floor_area * multiplier

            if emissions:
                key = code[:20]
            elif grouped:
                key = building.typology
            else:
                key = code

            if key not in output:
                output[key] = 0

            output[key] += building_sqm

            if log_empty and building_sqm <= 0 and not building.is_ref:
                log_warning(
                    f'Capacity of "{code}" is {building_sqm}. Useful floor area per building: {building_sqm:4.1f}, '
                    + f"Number of Buildings: {num_buildings:5.0f}, Occupancy: {multiplier:3.2f}"
                )

        return output

    def get_capacity(self, year: int) -> dict[str, int]:
        """This gets the total capacity occupied by a typology, possibly grouped by typology."""
        output: dict[str, int] = {typology: 0 for typology in RESIDENTIAL_TYPOLOGIES}

        for building in self.buildings.values():
            if not building.is_residential:
                continue

            output[building.typology] += building.get_capacity(year)

        return output

    def get_total_area(self, year: int, relevant_typologies: list[str] | None = None) -> float:
        """This gets the total area of the stock."""
        idx = year - self.scenario.data_start

        if relevant_typologies is not None:
            return sum(
                building.building_counts[idx] * building.useful_floor_area_per_building
                for building in self.buildings.values()
                if building.typology in relevant_typologies
            )

        return sum(
            building.building_counts[idx] * building.useful_floor_area_per_building
            for building in self.buildings.values()
        )

    def get_construction_area(self, year: int, relevant_typologies: list[str] | None = None) -> float:
        """This gets the constructed area for a year of the stock."""
        idx = year - self.scenario.start

        if relevant_typologies is not None:
            return sum(
                building.construction_counts[idx] * building.useful_floor_area_per_building
                for building in self.buildings.values()
                if building.typology in relevant_typologies and building.is_new
            )

        return sum(
            building.construction_counts[idx] * building.useful_floor_area_per_building
            for building in self.buildings.values()
            if building.is_new
        )

    def get_demolition_area(self, year: int, relevant_typologies: list[str] | None = None) -> float:
        """This gets the constructed area for a year of the stock."""
        idx = year - self.scenario.start

        if relevant_typologies is not None:
            return sum(
                building.demolition_counts[idx] * building.useful_floor_area_per_building
                for building in self.buildings.values()
                if building.typology in relevant_typologies
            )

        return sum(
            building.demolition_counts[idx] * building.useful_floor_area_per_building
            for building in self.buildings.values()
        )

    def get_refurbishment_area(self, year: int, relevant_typologies: list[str] | None = None) -> float:
        """This gets the refurbished area for a year of the stock."""
        idx = year - self.scenario.start

        if relevant_typologies is not None:
            return sum(
                building.construction_counts[idx] * building.useful_floor_area_per_building
                for building in self.buildings.values()
                if building.typology in relevant_typologies and building.is_ref
            )

        return sum(
            building.construction_counts[idx] * building.useful_floor_area_per_building
            for building in self.buildings.values()
            if building.is_ref
        )

    @time_function("Activity")
    def run_prediction(self, is_generation_run: bool) -> None:
        """Runs the full building stock prediction."""

        base_area = sum(
            area
            for typology, area in self.get_floor_area_by_typology(self.scenario.data_end).items()
            if typology in NON_RESIDENTIAL_TYPOLOGIES
        )
        con_rate = load_minimum_construction_rate(self.scenario.country_code, residential=False)

        fixed_increase = base_area * con_rate * FIXED_CONSTRUCTION_AREA_MULTIPLIER

        log_info(f"Default fixed area increase: {fixed_increase}")

        print("Predicting Building stock... ", end="")

        for year in range(self.scenario.start, self.scenario.end + 1):
            # self.validate(year - 1)

            self.advance_year(year)
            self.validate(year)

            # Calculate demolitions for current year and update the current building stock
            dem_area = do_demolitions(year, self.buildings)
            self.validate(year)

            # # Calculate refurbishments for current year and update the current building stock
            do_refurbishments(year, self.buildings, self.scenario, self.refurbishable_buildings)
            self.validate(year)

            # # Calculate constructions for current year and update the current building stock
            con_area = do_constructions(year, self.buildings, self.scenario, self.construction_stats, is_generation_run)
            self.validate(year)

            log_info(f"{year} Constructed area: {con_area}, Demolished area: {dem_area}")

        print("done.")

    def advance_year(self, year: int) -> None:
        """Advances the year of the building stock and copies data from the last year."""
        for building in self.buildings.values():
            building.advance_year(year)

    def validate(self, year: int) -> None:
        """Validates the building stock data."""
        for building in self.buildings.values():
            building.validate(year)

    def get_final_dataframes(
        self, recalculate: bool = False
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """
        This function gets the DataFrames for storing in this order\n:
        selected_building_stock, selected_demolitions, selected_refurbishments, selected_constructions
        """
        if not recalculate and self.collected_dataframes is not None:
            return self.collected_dataframes

        cols = ["code"] + [str(year) for year in range(self.scenario.start, self.scenario.end + 1)]

        stock_data = [
            [code] + build.building_counts[self.scenario.start - self.scenario.data_start :]
            for code, build in self.buildings.items()
        ]
        stock = pl.DataFrame(stock_data, schema=cols, orient="row")
        stock = stock.filter(stock.select(pl.exclude("code")).sum_horizontal() != 0)

        dem_data = [[code] + build.demolition_counts for code, build in self.buildings.items()]
        dem = pl.DataFrame(dem_data, schema=cols, orient="row")
        dem = dem.filter(dem.select(pl.exclude("code")).sum_horizontal() != 0)

        ref_data = [[code] + build.construction_counts for code, build in self.buildings.items() if build.is_ref]
        ref = pl.DataFrame(ref_data, schema=cols, orient="row")
        ref = ref.filter(ref.select(pl.exclude("code")).sum_horizontal() != 0)

        con_data = [[code] + build.construction_counts for code, build in self.buildings.items() if build.is_new]
        con = pl.DataFrame(con_data, schema=cols, orient="row")
        con = con.filter(con.select(pl.exclude("code")).sum_horizontal() != 0)

        self.collected_dataframes = (stock, dem, ref, con)

        return self.collected_dataframes

    def store_data(self, path: str) -> None:
        """Store some class data for debugging purposes."""
        stock, demos, refur, const = self.get_final_dataframes()

        store_building_stock(stock, path)
        store_demolitions(demos, path)
        store_refurbishments(refur, path)
        store_construction(const, path)

    def store_virtual_population(self) -> None:
        """Store some class data for debugging purposes."""
        data_end = self.scenario.data_end

        sqm_per_person = {
            typology: sqm / self.scenario.population[data_end]
            for typology, sqm in self.get_floor_area_by_typology(data_end).items()
        }

        v_pop_data: dict[int, int] = {data_end: self.scenario.population[data_end]}

        for year in range(self.scenario.start, self.scenario.end + 1):
            area = self.get_floor_area_by_typology(year)

            capacities: dict[str, float] = {
                typology: area[typology] / sqm_per_person[typology] for typology in NON_RESIDENTIAL_TYPOLOGIES
            }

            v_pop_data[year] = int(sum(capacities.values()) / len(capacities))
            assert v_pop_data[year] > 0

        store_virtual_population(v_pop_data, self.scenario.country_code)
