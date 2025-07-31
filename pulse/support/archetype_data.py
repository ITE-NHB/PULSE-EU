"""
archetype_data.py
-----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains functions that get data for the archetypes.
"""

from typing import NamedTuple, TypeAlias
import polars as pl

from pulse.support.file_interaction import (
    load_archetype_dataframe,
    load_floor_area_statistics,
    load_population_statistics,
    load_weibull,
)

### General imports and defines ###

from pulse.support.logging import log_info, log_warning
from pulse.support.defines import COUNTRY_CODE_TO_COUNTRY, NEW_CON_EPOCH, RESIDENTIAL_TYPOLOGIES


### Global variables to store data ###

__BUILDING_STOCK_DATA: pl.DataFrame | None = None
"""The DataFrame containing the data present in the parquet file.\n
!!!DO NOT ACCESS OUTSIDE OF file_interactions.py!!!"""


_EpochListType: TypeAlias = list[tuple[int, int]]
_UsageDistributionType: TypeAlias = dict[str, tuple[float, float, float]]
_CapacitiesType: TypeAlias = dict[str, float]
_UsefulFloorAreasType: TypeAlias = dict[str, float]
_StockCodesType: TypeAlias = list[str]
_HeatingSystemDistr: TypeAlias = dict[str, dict[str, float]]


class _BuildingStockInfo(NamedTuple):
    stock_codes: _StockCodesType
    residential_epochs: _EpochListType
    non_residential_epochs: _EpochListType
    usage_distribution: _UsageDistributionType
    capacities: _CapacitiesType
    floor_areas: _UsefulFloorAreasType
    heating_system_distribution: _HeatingSystemDistr


__BUILDING_STOCK_INFO: _BuildingStockInfo = _BuildingStockInfo([], [], [], {}, {}, {}, {})

__COUNTRY_DATA = None
"""An object containing static data on the country, extracted from the parquet file.\n
!!!DO NOT ACCESS OUTSIDE OF file_interactions.py!!!"""


### Initialization functions ###


def __init_buildingstock_dataframe(country: str) -> None:
    """This function initializes the Building Stock DataFrame."""
    global __BUILDING_STOCK_DATA

    __BUILDING_STOCK_DATA = load_archetype_dataframe(country)


def __calculate_epochs() -> tuple[_EpochListType, _EpochListType]:
    """Calculates the unique epochs for residential and non-residential archetypes."""
    df = get_buildingstock_dataframe()

    # Epoch lists
    residential_epoch_list: _EpochListType = []
    non_residential_epoch_list: _EpochListType = []

    # Create Residential epoch list
    unique_epochs = (
        df.filter(pl.col("archetype_name").str.contains("SFH", literal=True))
        .select(pl.col("archetype_name").str.slice(7, 9).alias("epochs"))
        .unique()
        .get_column("epochs")
        .sort(multithreaded=False)
    )

    residential_epoch_list = [(int(epoch[:4]), int(epoch[-4:])) for epoch in unique_epochs]

    # Create Non-Residential epoch list
    unique_epochs = (
        df.filter(pl.col("archetype_name").str.contains("OFF", literal=True))
        .select(pl.col("archetype_name").str.slice(7, 9).alias("epochs"))
        .unique()
        .get_column("epochs")
        .sort(multithreaded=False)
    )

    non_residential_epoch_list = [(int(epoch[:4]), int(epoch[-4:])) for epoch in unique_epochs]

    # Make sure the data hasn't changed! the last epoch should be 2010-2019 if new buildings are 2020-2050!
    new_epoch_res = residential_epoch_list.pop()
    new_epoch_non = non_residential_epoch_list.pop()
    assert new_epoch_res == new_epoch_non == NEW_CON_EPOCH

    assert residential_epoch_list[0][0] == non_residential_epoch_list[0][0], "The stock start must match!"
    assert residential_epoch_list[-1][1] == non_residential_epoch_list[-1][1], "The stock end must match!"

    return (residential_epoch_list, non_residential_epoch_list)


def __calculate_usages() -> _UsageDistributionType:
    """Calculates the usage distribution for each code."""
    df = get_buildingstock_dataframe()

    # Usage distribution
    usage_distributions: _UsageDistributionType = {}

    relevant = df.select(
        pl.col("archetype_name"), pl.col("occupied"), pl.col("secondary dwellings/units and others"), pl.col("vacant")
    )

    for code, occupied, secondary, vacant in relevant.iter_rows():
        assert isinstance(code, str)
        epoch = int(code[7:11])

        # Skip the NEW buildings because the occupancy is NaN
        if epoch == NEW_CON_EPOCH[0]:
            continue

        assert (
            isinstance(occupied, float) and isinstance(secondary, float) and isinstance(vacant, float)
        ), f"Occupancy values must be floats, but are {occupied}, {secondary}, {vacant} for {code}!"
        assert occupied >= 0 and secondary >= 0 and vacant >= 0

        summed = occupied + secondary + vacant or 1.0

        occupancy_rate = occupied / summed
        secondary_rate = secondary / summed
        vacancy_rate = vacant / summed

        relevant_code = code[:16]

        previous_usage = usage_distributions.setdefault(relevant_code, (occupancy_rate, secondary_rate, vacancy_rate))

        # Float comparison, so we round
        assert round(previous_usage[0] - occupancy_rate, 10) == 0
        assert round(previous_usage[1] - secondary_rate, 10) == 0
        assert round(previous_usage[2] - vacancy_rate, 10) == 0

        if occupancy_rate < 0.30:
            log_warning(f'Occupancy of "{relevant_code}" was evaluated to {occupancy_rate*100:.2f}%')
        elif occupancy_rate < 0.02:
            log_warning(
                f'Occupancy of "{relevant_code}" is {occupancy_rate*100:.2f}% '
                + f"occupied: {occupied}, vacant: {vacant}, secondary: {secondary}"
            )

        assert round(sum(usage_distributions[relevant_code]), 10) == 1, "Why don't the probabilities add to 1? :sob:"

    return usage_distributions


def __calculate_capacities() -> _CapacitiesType:
    """Calculates the capacity for each code."""
    df = get_buildingstock_dataframe()

    # Building capacities
    capacities: _CapacitiesType = {}

    relevant = df.filter(pl.col("archetype_name").str.contains_any(RESIDENTIAL_TYPOLOGIES)).select(
        pl.col("archetype_name"), pl.col("number of users")
    )

    for code, capacity in relevant.iter_rows():
        assert isinstance(code, str)
        assert isinstance(capacity, float)

        relevant_code = code[:16]

        previous_cap = capacities.setdefault(relevant_code, capacity)

        assert previous_cap == capacity, "The capacities in the same epoch and typology must match!"

        assert capacity >= 0, f"Number of users may NEVER be negative, but is {capacity} for {code}!"

        if capacity == 0:
            log_warning(f'Inhabitant capacity of "{code}" is {capacity}.')

    return capacities


def __calculate_floor_areas() -> _UsefulFloorAreasType:
    """Calculates the useful floor area for each code."""
    df = get_buildingstock_dataframe()

    # Floor areas
    floor_areas: _UsefulFloorAreasType = {}

    relevant = df.select(pl.col("archetype_name"), pl.col("reference building useful floor area"))

    for code, floor_area in relevant.iter_rows():
        assert isinstance(code, str)
        assert isinstance(floor_area, float)

        relevant_code = code[:16]

        previous_floor = floor_areas.setdefault(relevant_code, floor_area)

        assert previous_floor == floor_area, "The floor area in the same epoch and typology must match!"

        assert floor_area >= 0, f"Floor area may NEVER be negative, but is {floor_area} for {code}!"

        if floor_area == 0:
            log_warning(f'Floor area of "{code}" is {floor_area}.')

    return floor_areas


def __calculate_stock_codes() -> _StockCodesType:
    """Calculates the list of stock codes."""
    df = get_buildingstock_dataframe()

    stock_codes: _StockCodesType = []

    codes = df.get_column("archetype_name")
    assert codes.is_unique().all()

    stock_codes = sorted(list(codes))

    return stock_codes


def __calculate_heating_distributions() -> _HeatingSystemDistr:
    """Calculates the heating system distribution for each code."""
    df = get_buildingstock_dataframe()

    # Heating system distributions
    hs_distributions: _HeatingSystemDistr = {}

    relevant = df.select(pl.col("archetype_name"), pl.col("HVAC concept - system share"))

    for code, hs_share in relevant.iter_rows():
        assert isinstance(code, str)
        assert isinstance(hs_share, float)

        sel = code[:-3]
        hs = code[-2:]

        hs_distr = hs_distributions.setdefault(sel, {})

        hs_distr[hs] = hs_share

        assert hs_share >= 0, f"Heating System share may NEVER be negative, but is {hs_share} for {code}!"

        if hs_share == 0:
            log_warning(f'Heating System share of "{code}" is {hs_share}.')

    for relevant_code, distribution in hs_distributions.items():
        summed = sum(distribution.values())
        if summed != 1:
            hs_distributions[relevant_code] = {hs: hs_share / summed for hs, hs_share in distribution.items()}

    assert all(
        round(sum(hs_shares.values()), 10) == 1 for hs_shares in hs_distributions.values()
    ), "All heating system distributions should add up to 1!"

    return hs_distributions


def __init_building_stock_info() -> None:
    """This function initializes the building stock info object."""
    global __BUILDING_STOCK_INFO

    __BUILDING_STOCK_INFO = _BuildingStockInfo(
        __calculate_stock_codes(),
        *__calculate_epochs(),
        __calculate_usages(),
        __calculate_capacities(),
        __calculate_floor_areas(),
        __calculate_heating_distributions(),
    )


def __init_country_data(country_code: str, last_data_year: int) -> None:
    """This function initializes the country data."""
    global __COUNTRY_DATA
    __COUNTRY_DATA = BasicCountryData(country_code, last_data_year)


### ArchetypeStockData general information ###


def get_buildingstock_dataframe() -> pl.DataFrame:
    """This function returns the DataFrame of the building stock data."""
    assert __BUILDING_STOCK_DATA is not None
    return __BUILDING_STOCK_DATA


def get_buildingstock_codes() -> list[str]:
    """This function returns the list of archetype codes available in the building stock data."""
    return __BUILDING_STOCK_INFO.stock_codes


def get_buildingstock_start_year() -> int:
    """This function returns the first year of the building stock data."""
    return __BUILDING_STOCK_INFO.residential_epochs[0][0]


def get_buildingstock_end_year() -> int:
    """This function returns the last year of the building stock data."""
    return __BUILDING_STOCK_INFO.residential_epochs[-1][1]


def get_residential_epochs() -> list[tuple[int, int]]:
    """This function gets a list of all residential building epochs"""
    return __BUILDING_STOCK_INFO.residential_epochs


def get_non_residential_epochs() -> list[tuple[int, int]]:
    """This function gets a list of all non-residential building epochs"""
    return __BUILDING_STOCK_INFO.non_residential_epochs


def get_last_building_epoch_start() -> int:
    """This function gets the epoch start of the last building."""
    return __BUILDING_STOCK_INFO.residential_epochs[-1][0]


def get_last_building_epoch_end() -> int:
    """This function gets the epoch end of the last building."""
    return __BUILDING_STOCK_INFO.residential_epochs[-1][1]


### Code specific data acquisition ###


def get_usage_distribution(code: str) -> tuple[float, float, float]:
    """This function gets the usage distribution for a building archetype specified by code.\n
    Arguments:
        code (string): The building code to get the distribution for.
    Returns:
        (list[float]): A list containing the distribution [0: occupied%, 1: secondary%, 2: vacant%]
    """
    if code[12:16] == str(NEW_CON_EPOCH[1]):
        start = str(get_last_building_epoch_start())
        end = str(get_last_building_epoch_end())
        code = code[:7] + f"{start}-{end}"
    else:
        code = code[:16]

    return __BUILDING_STOCK_INFO.usage_distribution[code]


def get_building_capacity(code: str) -> float:
    """This function gets the amount of people to fit into the building specified by the code."""
    # BE-ABL-1850-1918-EXB-00-00-01
    if code[12:16] == str(NEW_CON_EPOCH[1]):
        start = str(get_last_building_epoch_start())
        end = str(get_last_building_epoch_end())
        code = code[:7] + f"{start}-{end}"
    else:
        code = code[:16]

    return __BUILDING_STOCK_INFO.capacities[code]


def get_useful_floor_area(code: str) -> float:
    """This function gets the amount of heated area per building specified by the code."""
    # BE-ABL-2020-2050-NEW-01-00-01
    if code[12:16] == str(NEW_CON_EPOCH[1]):
        start = str(get_last_building_epoch_start())
        end = str(get_last_building_epoch_end())
        code = code[:7] + f"{start}-{end}"
    else:
        code = code[:16]

    return __BUILDING_STOCK_INFO.floor_areas[code]


def get_heating_system_distribution(code: str) -> dict[str, float]:
    """This function gets the amount of people to fit into the building specified by the code."""
    # BE-ABL-1850-1918-EXB-00-00-01
    code = code[:21] + "00" + code[23:26]

    return __BUILDING_STOCK_INFO.heating_system_distribution[code]


### Country specific data calculations ###


def get_std_avg_floor_area(year: int) -> float:
    """This function returns the inital average square meters."""
    assert __COUNTRY_DATA is not None

    return __COUNTRY_DATA.get_std_average_square_meters(year)


def get_initial_std_avg_floor_area() -> float:
    """This function returns the inital average square meters."""
    assert __COUNTRY_DATA is not None

    return __COUNTRY_DATA.initial_std_avg_sqm


def get_std_population(year: int) -> int:
    """This function gets the standard population for a specified year."""
    assert __COUNTRY_DATA is not None

    return __COUNTRY_DATA.get_population(year)


def get_std_population_statistics() -> dict[int, int]:
    """This function gets the full population statistics."""
    assert __COUNTRY_DATA is not None

    return __COUNTRY_DATA.std_population_development


def get_weibull() -> dict[str, dict[str, list[float]]]:
    """This function gets the weibull data specified."""
    assert __COUNTRY_DATA is not None

    return __COUNTRY_DATA.get_weibull()


class BasicCountryData:
    """
    A class that stores some data to quickly access without having to recalculate.\n
    Members:
    __BasicCountryData.name: The name of the country
    __BasicCountryData.initial_avg_sqm: The average square meters of living area per person at the end of the data
    __BasicCountryData.initial_population: The population at the end of the data
    """

    code: str
    name: str
    std_average_square_meters: dict[int, float]
    initial_std_avg_sqm: float
    std_population_development: dict[int, int]
    weibull: dict[str, dict[str, list[float]]]

    def __init__(self, code: str, last_data_year: int) -> None:
        self.code = code
        self.name = COUNTRY_CODE_TO_COUNTRY[self.code]

        self.std_average_square_meters = load_floor_area_statistics(self.code)
        self.initial_std_avg_sqm = self.std_average_square_meters[last_data_year]

        self.std_population_development = load_population_statistics(self.code)

        self.weibull = load_weibull(self.code)

    def get_std_average_square_meters(self, year: int) -> float:
        """Get the standard average square meters for a given year (If there was no scenario)."""
        return self.std_average_square_meters[year]

    def get_population(self, year: int) -> int:
        """Get the population for a specified year (If there was no scenario)."""
        return self.std_population_development[year]

    def get_weibull(self) -> dict[str, dict[str, list[float]]]:
        """Get the countries' weibull data."""
        return self.weibull


### Initializer ###


def init_helpers(country_code: str, last_data_year: int) -> None:
    """A function to initialize all members."""
    __init_buildingstock_dataframe(country_code)
    __init_building_stock_info()
    __init_country_data(country_code, last_data_year)

    log_info("Successfully initialized BuildingStock and Country data.")
