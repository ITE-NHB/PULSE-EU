"""
emission_calculation.py
-----------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains calculations pertaining to emissions.
"""

import polars as pl

from pulse.config import (
    B2B4B6_EMISSIONS_PATH,
    CONSTRUCTION_EMISSIONS_PATH,
    DEMOLITION_EMISSIONS_PATH,
    EMISSIONS_FOLDER,
    REFURBISHMENT_EMISSIONS_PATH,
    SCENARIORIZED_EMISSIONS_PATH,
)

from pulse.support.arguments import runArgs

from pulse.support.defines import B_STAGES, BASIC_INDICATORS, CON_STAGES, DEM_STAGES, OPTIONAL_INDICATORS, REF_STAGES
from pulse.support.file_interaction import load_archetype_emissions, store_emissions
from pulse.support.logging import log_info
from pulse.support.ui import time_function


from pulse.activities import BuildingStock, Scenario

from .emission_preprocessing import add_preprocessing_columns, load_parameters, preprocess_emissions

from . import export


def get_activity_dfs(building_stock: BuildingStock, scenario: Scenario) -> dict[str, pl.DataFrame]:
    """This function gets the properly formatted polars activity DataFrames from the supplied dictionary."""
    stock, dem, ref, con = building_stock.get_final_dataframes()
    activities = {"Building Stock": stock, "Demolitions": dem, "Refurbishments": ref, "Constructions": con}

    extra_data: dict[str, list[str | int | float]] = {
        "code": [],
        "Year": [],
        "area_per_building": [],
        "usage": [],
    }

    for code, building in building_stock.buildings.items():
        for year in range(scenario.start, scenario.end + 1, scenario.step_size):
            extra_data["code"].append(code)
            extra_data["Year"].append(year)
            extra_data["area_per_building"].append(building.useful_floor_area_per_building)
            extra_data["usage"].append(building.get_share_of_usage(year, artificial_usage=(1, 0, 0)))

    extra_data_df = pl.DataFrame(extra_data)

    relevant_columns = ["code"] + [str(y) for y in range(scenario.start, scenario.end + 1, scenario.step_size)]

    for dftype, df in activities.items():
        if dftype == "Building Stock":
            calculation_expression = pl.col("Number of buildings") * pl.col("area_per_building") * pl.col("usage")
        else:
            calculation_expression = pl.col("Number of buildings") * pl.col("area_per_building")

        activities[dftype] = (
            df.select(relevant_columns)
            # Melt the df to have 3 columns: code, Year, Number of buildings
            .melt(id_vars="code", variable_name="Year", value_name="Number of buildings")
            #
            .filter(pl.col("Number of buildings").ne(0))
            .with_columns(pl.col("Year").cast(pl.Int32))
            #
            .join(extra_data_df, on=["code", "Year"], how="left")
            .with_columns(calculation_expression.alias("Area of buildings"))
            #
            .drop("Number of buildings", "area_per_building", "usage")
        )

    for dftype, col_df in activities.items():
        assert not col_df.get_column("Area of buildings").is_nan().any()

    return activities


def process_emissions_data(
    emissions: pl.LazyFrame, buildings_df: pl.DataFrame, stages: list[str], indicators: list[str], year: int
) -> pl.LazyFrame:
    """Multiplies the supplied emissions with the number of buildings in the given year."""
    # log_missing_archetypes(buildings_df, emissions)

    relevant_buildings = buildings_df.lazy().filter(pl.col("Year").eq(year))
    relevant_emissions = emissions.filter(pl.col("LCS_EN15978").is_in(stages))

    # Merge the transformed building data with the filtered parquet data
    result = relevant_emissions.join(
        relevant_buildings,
        how="inner",
        left_on="building_archetype_code",
        right_on="code",
    )

    # Multiply each indicator column by 'Area of buildings'
    result = result.with_columns([pl.col(col) * pl.col("Area of buildings") for col in indicators])

    return result


@time_function("Emission")
def calculate_emissions(
    folder: str,
    scenario: Scenario,
    building_stock: BuildingStock,
    is_generation_run: bool,
) -> None:
    """This function does the processing."""
    print("Emission processing... ", end="")
    args = runArgs

    full_output = args.full_output
    create_pyam_export = args.create_pyam_export
    output_folder = args.output_folder

    activities = get_activity_dfs(building_stock, scenario)

    if is_generation_run:
        parameters = []
    else:
        parameters = load_parameters(scenario, output_folder, full_output)

    # Drop optional indicators
    irrelevant_indicators = OPTIONAL_INDICATORS if not full_output else []

    ### Getting the input emission data ###
    emissions = load_archetype_emissions(scenario.country_code)
    emissions = emissions.drop(irrelevant_indicators)
    emissions = add_preprocessing_columns(emissions, parameters)

    building_stock_emissions_list: list[pl.LazyFrame] = []
    demolitions_emissions_list: list[pl.LazyFrame] = []
    refurbishments_emissions_list: list[pl.LazyFrame] = []
    constructions_emissions_list: list[pl.LazyFrame] = []

    indicators = BASIC_INDICATORS + (OPTIONAL_INDICATORS if full_output else [])

    ### The emission calculations ###
    for year in range(scenario.start, scenario.end + 1, scenario.step_size):
        preprocessed_lazyframe = preprocess_emissions(emissions, parameters, year)

        if args.store_preprocessing_files:
            store_emissions(preprocessed_lazyframe, SCENARIORIZED_EMISSIONS_PATH.format(folder, year))

        new_building_emissions = process_emissions_data(
            emissions=preprocessed_lazyframe,
            buildings_df=activities["Building Stock"],
            stages=B_STAGES,
            indicators=indicators,
            year=year,
        )

        new_demolition_emissions = process_emissions_data(
            emissions=preprocessed_lazyframe,
            buildings_df=activities["Demolitions"],
            stages=DEM_STAGES,
            indicators=indicators,
            year=year,
        )

        new_refurbishment_emissions = process_emissions_data(
            emissions=preprocessed_lazyframe,
            buildings_df=activities["Refurbishments"],
            stages=REF_STAGES,
            indicators=indicators,
            year=year,
        )

        new_construction_emissions = process_emissions_data(
            emissions=preprocessed_lazyframe,
            buildings_df=activities["Constructions"],
            stages=CON_STAGES,
            indicators=indicators,
            year=year,
        )

        building_stock_emissions_list.append(new_building_emissions)
        demolitions_emissions_list.append(new_demolition_emissions)
        refurbishments_emissions_list.append(new_refurbishment_emissions)
        constructions_emissions_list.append(new_construction_emissions)

        if is_generation_run:
            break

    building_stock_emissions: pl.LazyFrame = pl.concat(building_stock_emissions_list).collect().lazy()
    demolitions_emissions: pl.LazyFrame = pl.concat(demolitions_emissions_list).collect().lazy()
    refurbishments_emissions: pl.LazyFrame = pl.concat(refurbishments_emissions_list).collect().lazy()
    constructions_emissions: pl.LazyFrame = pl.concat(constructions_emissions_list).collect().lazy()

    if args.store_activity_emission_files:
        print("storing... ", end="")
        store_emissions(building_stock_emissions, B2B4B6_EMISSIONS_PATH.format(folder))
        store_emissions(demolitions_emissions, DEMOLITION_EMISSIONS_PATH.format(folder))
        store_emissions(refurbishments_emissions, REFURBISHMENT_EMISSIONS_PATH.format(folder))
        store_emissions(constructions_emissions, CONSTRUCTION_EMISSIONS_PATH.format(folder))

        log_info(f"Stored intermediate emission files to: {EMISSIONS_FOLDER.format(folder)}")

    all_emissions = pl.concat(
        [building_stock_emissions, demolitions_emissions, refurbishments_emissions, constructions_emissions]
    )

    print("export... ", end="")
    export.generate_emission_export(all_emissions, building_stock, folder, full_output)

    if create_pyam_export:
        print("pyam... ", end="")
        export.generate_pyam_export(folder, scenario, building_stock, all_emissions, is_generation_run)

    print("done.")
