"""
pyam_export.py
--------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This generates the pyam export and stores it.
"""

import polars as pl

from pulse.activities import BuildingStock, Scenario
from pulse.support.file_interaction import store_pyam_export

from .defines import (
    RES_AND_COMM,
    RESIDENTIAL,
    COMMERCIAL,
    DEMOLITION,
    REFURBISHMENT,
    CONSTRUCTION,
    STOCK,
    DEMOLISHED,
    RENOVATED,
    NEW,
)

from .gwp_emissions import calculate_gwp
from .calculate_activity_ratios import calculate_rate
from .calc_useful_floor_area import calculate_useful_floor_area
from .population import calculate_population


def generate_pyam_export(
    folder: str,
    scenario: Scenario,
    building_stock: BuildingStock,
    emissions: pl.LazyFrame | None = None,
    is_generation_run: bool = False,
) -> None:
    """
    This function generates the pyam export and stores it.\n
    If no emission data is supplied, only the activity data is exported.
    """
    dataframes: list[pl.DataFrame] = []

    # Generate the GWP summary
    if emissions is not None:
        dataframes.append(calculate_gwp(scenario, emissions, is_generation_run))

    # # Generate the building rates
    dataframes.append(calculate_rate(scenario, building_stock, CONSTRUCTION, RES_AND_COMM, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, CONSTRUCTION, RESIDENTIAL, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, CONSTRUCTION, COMMERCIAL, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, REFURBISHMENT, RES_AND_COMM, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, REFURBISHMENT, RESIDENTIAL, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, REFURBISHMENT, COMMERCIAL, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, DEMOLITION, RES_AND_COMM, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, DEMOLITION, RESIDENTIAL, is_generation_run))
    dataframes.append(calculate_rate(scenario, building_stock, DEMOLITION, COMMERCIAL, is_generation_run))

    # Generate the useful floor area for existing buildings
    dataframes.append(calculate_useful_floor_area(scenario, building_stock, STOCK, is_generation_run))
    dataframes.append(calculate_useful_floor_area(scenario, building_stock, NEW, is_generation_run))
    dataframes.append(calculate_useful_floor_area(scenario, building_stock, DEMOLISHED, is_generation_run))
    dataframes.append(calculate_useful_floor_area(scenario, building_stock, RENOVATED, is_generation_run))

    # # Generate the population data
    dataframes.append(calculate_population(scenario, building_stock, is_generation_run))

    # Concatenate the DataFrames
    final_df = pl.concat(dataframes)

    height = final_df.height

    final_df = final_df.insert_column(0, pl.Series("Model", ["SLiCE + PULSE-EU"] * height))
    final_df = final_df.insert_column(1, pl.Series("Scenario", [scenario.name] * height))
    final_df = final_df.insert_column(2, pl.Series("Region", [scenario.country_code.upper()] * height))

    store_pyam_export(final_df, folder, scenario.name)
