"""
population.py
-------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The population data parser.
"""

import polars as pl

from pulse.support.defines import COUNTRY_TO_COUNTRY_CODE
from pulse.config import POPULATION_1800_2021_RAW_DATA, POPULATION_2019_2050_RAW_DATA, POPULATION_STATISTIC_PATH
from pulse.support.file_interaction import read_dataframe_from_excel, write_dict_to_json


def get_population_data(country: str) -> tuple[dict[int, int], dict[int, int]]:
    """This function gets the population data from the 3 files."""

    def get_population_dataframes() -> tuple[pl.DataFrame, pl.DataFrame]:
        """Gets the three population files as dataframes."""
        pop_1800_2021 = pl.read_csv(POPULATION_1800_2021_RAW_DATA)
        pop_1800_2021 = pop_1800_2021.with_columns(pl.first().replace_strict(COUNTRY_TO_COUNTRY_CODE))

        pop_2019_2050 = read_dataframe_from_excel(POPULATION_2019_2050_RAW_DATA, None)
        pop_2019_2050 = pop_2019_2050.with_columns(pl.first().replace_strict(COUNTRY_TO_COUNTRY_CODE))

        return (pop_1800_2021, pop_2019_2050)

    dataframe_1800_2021, dataframe_2019_2050 = get_population_dataframes()

    population_1800_2021: dict[int, int] = {}
    population_2019_2050: dict[int, int] = {}

    for row in dataframe_1800_2021.filter(pl.first() == country).iter_rows(named=True):
        population_1800_2021[int(row["Year"])] = row["Population (historical estimates)"]

    pop_data = zip(
        dataframe_2019_2050.select(pl.exclude("Country")).columns,
        dataframe_2019_2050.filter(pl.first() == country).select(pl.exclude("Country")).row(0),
    )

    for year, pop in pop_data:
        population_2019_2050[int(year)] = pop

    return (population_1800_2021, population_2019_2050)


def interpolate_data(
    population_1800_2021: dict[int, int],
    population_2019_2050: dict[int, int],
) -> dict[int, int]:
    """
    This applies the chosen interpolation and selection strategies to the two dictionaries:
    1840 - 2010: population_1800_2021
    2011 - 2018: Interpolating from population_1800_2021[2010] to population_2019_2050[2019]
    2019 - 2050: population_2019_2050
    """

    def lerp(start: int, end: int, factor: float) -> int:
        return int(start + (end - start) * factor)

    pop_dict: dict[int, int] = {}

    for year in range(1840, 2010 + 1):
        pop_dict[year] = population_1800_2021[year]

    pop_2010 = population_1800_2021[2010]
    pop_2019 = population_2019_2050[2019]

    for year in range(2011, 2018 + 1):
        factor = (year - 2010) / (2019 - 2010)
        pop_dict[year] = lerp(pop_2010, pop_2019, factor)

    for year in range(2019, 2050 + 1):
        pop_dict[year] = population_2019_2050[year]

    return pop_dict


def parse_population(countries: list[str]) -> None:
    """This function parses the population files."""
    for country in countries:
        interpolated_data = interpolate_data(*get_population_data(country))

        write_dict_to_json(interpolated_data, POPULATION_STATISTIC_PATH.format(country).lower())
