"""
scenario_parameters.py
----------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains scenario parameters.
"""

from abc import ABC

from typing import Any, TypeVar, Generic, TypeAlias

from pulse.support.archetype_data import get_std_avg_floor_area, get_std_population
from pulse.support.defines import (
    DEEP,
    LIGHT,
    MEDIUM,
    TYPOLOGIES,
    RESIDENTIAL_TYPOLOGIES,
    NON_RESIDENTIAL_TYPOLOGIES,
)
from pulse.support.file_interaction import load_std_refurbishment_rates
from pulse.support.logging import log_info

POPULATION_OPTION = "Population"
FLOOR_AREA_OPTION = "Floor area per person (NFA)"

USE_OF_EMPTY = "Use of empty"

INCREASE_NEW_NON_RES = "Increase new Non-Residential"
SHARE_OF_NEW_RES = "Share of new Residential"

SHARE_CON_EP_HEA_OPTION = "Share of new {} {} {}"

SHARE_OF_RES_REF = "Share of Residential Refurbishments"
SHARE_OF_NON_RES_REF = "Share of Non-Residential Refurbishments"
REPURPOSE_DEMO_OPTION = "Share of buildings repurposed"


# Define a generic type variable
T = TypeVar("T")

NestedDict: TypeAlias = dict[str, dict[str, T]]


class ScenarioParameter(ABC, Generic[T]):
    """The basic scenario option object."""

    data: list[T]
    """The data this Parameter holds."""
    unit: str
    """The unit of the variable."""
    start: int
    """The start year of the scenario."""
    end: int
    """The end year of the scenario."""

    def __init__(self, unit: str, start: int, end: int) -> None:
        assert isinstance(unit, str)
        assert isinstance(start, int)
        assert isinstance(end, int)

        self.unit: str = unit
        self.start: int = start
        self.end: int = end

    @property
    def year_count(self) -> int:
        """The amount of years of the scenario."""
        return self.end - self.start + 1

    def __str__(self) -> str:
        """This gets the string version of the Option"""
        return f"{self.__class__.__name__} (Unit: {self.unit})"

    def _get_index(self, year: int) -> int:
        """A function that gets the index of a year from the ScenarioOption"""
        if not self.start <= year <= self.end:
            raise IndexError(f"Index {year} is out of range of [{self.start}, {self.end}]")

        return year - self.start

    def set_data(self, data: list[T]) -> None:
        """A function that sets the data of the ScenarioOption"""
        assert len(data) == self.year_count
        self.data = data

    def __getitem__(self, year: int) -> T:
        """A function that gets data from the ScenarioOption"""
        return self.data[self._get_index(year)]


class PopulationParameter(ScenarioParameter[int]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [0] * self.year_count

        for year in range(self.year_count):
            final_data[year] = get_std_population(self.start + year)

        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("p", years[0] - 1, years[1])

        if POPULATION_OPTION not in data:
            self.__init_default_values__()
            log_info("Initialized population with default values.")
            return

        local_data = data.pop(POPULATION_OPTION)

        final_data: list[int] = [0] * self.year_count
        final_data[0] = get_std_population(self.start)

        for year in range(1, self.year_count):
            population = local_data[str(self.start + year)]

            assert isinstance(population, int)
            final_data[year] = population

        self.set_data(final_data)


class FloorAreaParameter(ScenarioParameter[float]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [0.0] * self.year_count

        for year in range(self.year_count):
            final_data[year] = get_std_avg_floor_area(self.start + year)

        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("m²", years[0], years[1])

        if FLOOR_AREA_OPTION not in data:
            self.__init_default_values__()
            log_info("Initialized floor area with default values.")
            return

        local_data = data.pop(FLOOR_AREA_OPTION)

        final_data: list[float] = [0.0] * self.year_count

        for year in range(self.year_count):
            floor_area = get_std_avg_floor_area(self.start + year) * local_data[str(self.start + year)] * 0.01

            assert isinstance(floor_area, (float, int))
            final_data[year] = float(floor_area)

        self.set_data(final_data)


class UseOfEmptyParameter(ScenarioParameter[tuple[float, float, float]]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [
            (
                0.0,
                0.0,
                0.0,
            )
        ] * self.year_count
        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("%", years[0] - 1, years[1])

        if not USE_OF_EMPTY in data:
            self.__init_default_values__()
            log_info("Initialized use of empty with default values.")
            return

        local_data = data.pop(USE_OF_EMPTY)

        # List mapping the year (0-based) to the use of empty data
        final_data: list[tuple[float, float, float]] = [(0, 0, 0)] * self.year_count

        # Read the empty dwelling data
        for year in range(1, self.year_count):
            use_of_empty = local_data[str(self.start + year)]

            assert len(use_of_empty) == 3
            assert all(isinstance(val, (float, int)) and 0 <= val <= 100 for val in use_of_empty)

            final_data[year] = (
                use_of_empty[0] * 0.01,
                use_of_empty[1] * 0.01,
                use_of_empty[2] * 0.01,
            )

        assert final_data[0] == (0, 0, 0), "The first year of the scenario should have no use of empty!"

        for year in range(1, self.year_count):

            error_string = "The scenario parameter is decreasing when it shouldnt be! {} in {}, {} in {}"

            present = final_data[year]
            past = final_data[year - 1]

            present_year = self.start + year

            assert past[0] <= present[0], error_string.format(past[0], present_year - 1, present[0], present_year)
            assert past[1] <= present[1], error_string.format(past[1], present_year - 1, present[1], present_year)
            assert past[2] <= present[2], error_string.format(past[2], present_year - 1, present[2], present_year)

        self.set_data(final_data)


class IncreaseNewParameter(ScenarioParameter[dict[str, float]]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [{typology: 0.0 for typology in NON_RESIDENTIAL_TYPOLOGIES}] * self.year_count
        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("%", years[0], years[1])

        if INCREASE_NEW_NON_RES not in data:
            self.__init_default_values__()
            log_info("Initialized increase new non-residential with default values.")
            return

        local_data = data.pop(INCREASE_NEW_NON_RES)

        # List mapping the year (0-based) to the use of empty data
        final_data: list[dict[str, float]] = []

        # Read all values (for each year (0 based), for each typology, for each type read {data * 0.01})
        for year in range(self.year_count):
            # Parse the increase of new data
            increase_new = local_data[str(self.start + year)]

            assert isinstance(increase_new, dict)
            assert all(typology in increase_new for typology in NON_RESIDENTIAL_TYPOLOGIES)
            assert all(isinstance(val, (float, int)) and 0 <= val <= 100 for val in increase_new.values())

            final_data.append({typology: increase * 0.01 for typology, increase in increase_new.items()})

        self.set_data(final_data)


class ShareOfNewParameter(ScenarioParameter[dict[str, float]]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [{typology: 0.0 for typology in RESIDENTIAL_TYPOLOGIES}] * self.year_count
        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("%", years[0], years[1])

        if SHARE_OF_NEW_RES not in data:
            self.__init_default_values__()
            log_info("Initialized share of new residential with default values.")
            return

        local_data = data.pop(SHARE_OF_NEW_RES)

        # List mapping the year (0-based) to the use of empty data
        final_data: list[dict[str, float]] = []

        # Read all values (for each year (0 based), for each typology, for each type read {data * 0.01})
        for year in range(self.year_count):
            # Parse the increase of new data
            share_of_new: dict[str, float] = local_data[str(self.start + year)]

            assert isinstance(share_of_new, dict)
            assert all(typology in share_of_new for typology in RESIDENTIAL_TYPOLOGIES)
            assert all(
                val is None or (isinstance(val, (float, int)) and 0 <= val <= 100) for val in share_of_new.values()
            )
            assert (
                sum(val for val in share_of_new.values() if val is not None) <= 100
            ), "The sum of the share of new typologies should be <= 100%!"

            final_data.append(
                {typology: ((share * 0.01) if share is not None else share) for typology, share in share_of_new.items()}
            )

        self.set_data(final_data)


class ShareOfConEpHeaParameter(ScenarioParameter[tuple[NestedDict[float], NestedDict[float], NestedDict[float]]]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data: list[tuple[NestedDict[float], NestedDict[float], NestedDict[float]]] = [
            (
                {typology: {} for typology in TYPOLOGIES},
                {typology: {} for typology in TYPOLOGIES},
                {typology: {} for typology in TYPOLOGIES},
            )
        ] * self.year_count

        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("%", years[0], years[1])
        # A dict that map from typology to a dict that then maps from construction/heating type to the year data
        local_con_data: dict[str, NestedDict[Any]] = {}
        local_ep_data: dict[str, NestedDict[Any]] = {}
        local_hea_data: dict[str, NestedDict[Any]] = {}

        for typology in TYPOLOGIES:
            local_con_data[typology] = {}
            local_ep_data[typology] = {}
            local_hea_data[typology] = {}

            filtered = [
                key for key in data.keys() if SHARE_CON_EP_HEA_OPTION.format(typology, "construction", "") in key
            ]

            for variable in filtered:
                local_con_data[typology][variable[-1:]] = data.pop(variable)

            filtered = [
                key for key in data.keys() if SHARE_CON_EP_HEA_OPTION.format(typology, "energy performance", "") in key
            ]

            for variable in filtered:
                local_ep_data[typology][variable[-1:]] = data.pop(variable)

            filtered = [key for key in data.keys() if SHARE_CON_EP_HEA_OPTION.format(typology, "heating", "") in key]

            for variable in filtered:
                local_hea_data[typology][variable[-2:]] = data.pop(variable)

        # A list of dicts (list index is year - start) that map from typology to a dict that then maps from
        # construction/heating type to the share of that type
        share_of_con: list[NestedDict[float]] = []
        share_of_ep: list[NestedDict[float]] = []
        share_of_hea: list[NestedDict[float]] = []

        # Read all values (for each year (0 based), for each typology, for each type read {data * 0.01})
        for year in range(self.year_count):
            share_of_con.append({})
            share_of_ep.append({})
            share_of_hea.append({})

            for typology in TYPOLOGIES:
                share_of_con[year][typology] = {}
                share_of_ep[year][typology] = {}
                share_of_hea[year][typology] = {}

                for c_type, c_values in local_con_data[typology].items():
                    value = c_values[str(self.start + year)] * 0.01
                    assert isinstance(value, (float, int))

                    share_of_con[year][typology][c_type] = value

                for ep_type, ep_values in local_ep_data[typology].items():
                    value = ep_values[str(self.start + year)] * 0.01
                    assert isinstance(value, (float, int))

                    share_of_ep[year][typology][ep_type] = value

                for h_type, h_values in local_hea_data[typology].items():
                    value = h_values[str(self.start + year)] * 0.01
                    assert isinstance(value, (float, int))

                    share_of_hea[year][typology][h_type] = value

        final_data = list(zip(share_of_con, share_of_ep, share_of_hea))

        self.set_data(final_data)


class ShareOfRefParameter(ScenarioParameter[dict[str, float]]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self, country: str, residential: bool) -> None:
        final_data = [load_std_refurbishment_rates(country, residential)] * self.year_count
        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int], country: str, residential: bool) -> None:
        super().__init__("%", years[0], years[1])

        key = SHARE_OF_RES_REF if residential else SHARE_OF_NON_RES_REF

        if key not in data:
            self.__init_default_values__(country=country, residential=residential)
            log_info(
                "Initialized share of "
                + ("residential" if residential else "non-residential")
                + " refurbishments with default values."
            )
            return

        local_data = data.pop(key)

        # List mapping the year (0-based) to the use of empty data
        final_data: list[dict[str, float]] = []

        default_values = load_std_refurbishment_rates(country, residential)

        # Read the empty dwelling data
        for year in range(self.year_count):
            # This data is required, since it now is now different for every country
            share_of_ref = local_data[str(self.start + year)]

            assert isinstance(share_of_ref, dict)
            assert all(isinstance(val, (float, int)) and 0 <= val <= 100 for val in share_of_ref.values())

            final_data.append(
                {
                    LIGHT: share_of_ref[LIGHT] * 0.01 if LIGHT in share_of_ref else default_values[LIGHT],
                    MEDIUM: share_of_ref[MEDIUM] * 0.01 if MEDIUM in share_of_ref else default_values[MEDIUM],
                    DEEP: share_of_ref[DEEP] * 0.01 if DEEP in share_of_ref else default_values[DEEP],
                }
            )

        self.set_data(final_data)


class RepurposeDemoParameter(ScenarioParameter[float]):
    """An option for a scenario, derived from base class ScenarioParameter."""

    def __init_default_values__(self) -> None:
        final_data = [0.0] * self.year_count
        self.set_data(final_data)

    def __init__(self, data: NestedDict[Any], years: tuple[int, int]) -> None:
        super().__init__("%", years[0], years[1])

        if REPURPOSE_DEMO_OPTION not in data:
            self.__init_default_values__()
            log_info("Initialized share of demolished buildings repurposed with default values.")
            return

        local_data = data.pop(REPURPOSE_DEMO_OPTION)

        # List mapping the year (0-based) to the use of empty data
        final_data = [0.0] * self.year_count

        # Read all values (for each year (0 based), for each typology, for each type read {data * 0.01})
        for year in range(self.year_count):
            # Parse the increase of new data
            repurpose_demo = local_data[str(self.start + year)]

            assert isinstance(repurpose_demo, (float, int)) and 0 <= repurpose_demo <= 100

            final_data[year] = repurpose_demo * 0.01

        self.set_data(final_data)
