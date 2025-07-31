"""
emission_export.py
------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the function for generating the emission export.
"""

import polars as pl
import polars.datatypes.classes as pl_types

from pulse.activities import BuildingStock
from pulse.activities.code import get_typology_from_code
from pulse.support.defines import (
    BASIC_INDICATORS,
    COUNTRY_TO_REGION,
    NON_RESIDENTIAL,
    OPTIONAL_INDICATORS,
    RESIDENTIAL,
    RESIDENTIAL_TYPOLOGIES,
    STAGE_TO_CATEGORY,
)
from pulse.support.file_interaction import load_scaling_factors, store_emissions_export

from .defines import NUM_OUTPUT_DIGITS, SORT_OUTPUT


def generate_first_columns(lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """This function generates columns that can only be done before grouping."""
    # "Area of buildings": "stock_floor_area_Mm2"

    lazyframe = lazyframe.with_columns(
        amount_material=pl.col("amount_material_kg_per_building") * pl.col("Area of buildings") / pl.lit(1_000_000_000)
    )

    return lazyframe


def drop_unnecessary_columns(lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """This function formats the DataFrame in the correct order for storage."""
    lazyframe = lazyframe.drop(
        "building_use_subtype_code",
        "stock_activity_type_code",
        "amount_material_kg_per_building",
        # "Area of buildings",
        # The rest is already dropped in the emissions calculations
    )

    return lazyframe


def group_code(lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """This function groups the code column."""
    # BE-ABL-2020-2050-NEW-00-00-01
    lazyframe = lazyframe.with_columns(
        pl.col("building_archetype_code").str.slice(20, 9).alias("code_extension"),
        pl.col("building_archetype_code").str.slice(0, 20).alias("building_archetype_code"),
    )

    # Naughty little insertion of column generation 😉

    relevant_cols = ["building_archetype_code", "Year", "LCS_EN15978"]

    lazyframe = (
        lazyframe.group_by(relevant_cols)
        .agg(
            stock_floor_area_Mm2=(
                pl.col("Area of buildings").filter(pl.col("code_extension").is_first_distinct()).sum()
                / pl.lit(1_000_000)
            )
        )
        .join(lazyframe, on=relevant_cols)
    )

    lazyframe = lazyframe.drop("Area of buildings", "code_extension")

    # End of naughty little insertion of column generation 😉

    relevant_cols = [
        "building_archetype_code",
        "Year",
        "LCS_EN15978",
        "element_class_generic_name",
        "techflow_name_mmg",
        "material_name_JRC_CDW",
        "activity_in_out",
        "stock_floor_area_Mm2",
    ]

    lazyframe = lazyframe.group_by(relevant_cols).sum()

    return lazyframe


def generate_final_columns(lazyframe: pl.LazyFrame, building_stock: BuildingStock, full_output: bool) -> pl.LazyFrame:
    """This function formats the DataFrame in the correct order for storage."""
    scenario = building_stock.scenario
    # Renaming columns
    lazyframe = lazyframe.rename({"Year": "stock_projection_year"})

    # Fixed values
    country_code = scenario.country_code
    region = COUNTRY_TO_REGION[scenario.country_code]

    # Mappings
    activity_type_mapping = {"E": "Existing buildings", "N": "New buildings", "R": "Refurbishment"}

    other_data_data: dict[str, list[str | int | float]] = {
        "stock_projection_year": [],
        "building_archetype_code": [],
        "LCS_EN15978": [],
        #
        "building_use_subtype_name": [],
        "building_use_type_name": [],
        "stock_activity_type_name": [],
        #
        "population_archetype": [],
        "population_country": [],
        #
        "floor_area_archetype": [],
        "floor_area_country": [],
        #
        "carbon_category": [],
    }

    for year in range(scenario.start, scenario.end + 1, scenario.step_size):
        population = scenario.population[year]
        capacity_by_typology = building_stock.get_capacity(year)
        total_capacity = sum(capacity_by_typology.values())
        pop_distribution = {typology: amount / total_capacity for typology, amount in capacity_by_typology.items()}

        floor_areas_by_code = building_stock.get_floor_area_by_typology(year, emissions=True)
        floor_area_country = sum(floor_areas_by_code.values()) / 1_000_000

        for code, area in floor_areas_by_code.items():
            typology = get_typology_from_code(code)
            is_residential = typology in RESIDENTIAL_TYPOLOGIES

            use_type = RESIDENTIAL if is_residential else NON_RESIDENTIAL
            activity_type = activity_type_mapping[code[17]]

            population_archetype = int(pop_distribution[typology] * population) if is_residential else 0
            population_country = population if is_residential else 0

            floor_area_archetype = area / 1_000_000

            for lcs, category in STAGE_TO_CATEGORY.items():
                other_data_data["stock_projection_year"].append(year)
                other_data_data["building_archetype_code"].append(code)
                other_data_data["LCS_EN15978"].append(lcs)

                other_data_data["building_use_subtype_name"].append(typology)
                other_data_data["building_use_type_name"].append(use_type)
                other_data_data["stock_activity_type_name"].append(activity_type)

                other_data_data["population_archetype"].append(population_archetype)
                other_data_data["population_country"].append(population_country)

                other_data_data["floor_area_archetype"].append(floor_area_archetype)
                other_data_data["floor_area_country"].append(floor_area_country)

                other_data_data["carbon_category"].append(category)

    other_data = pl.LazyFrame(other_data_data)

    lazyframe = lazyframe.with_columns(
        pl.lit(country_code).alias("country_name"),
        pl.lit(region).alias("stock_region_name"),
    )

    lazyframe = lazyframe.join(
        other_data, on=["stock_projection_year", "building_archetype_code", "LCS_EN15978"], how="left"
    )

    if full_output:
        # Calibration Factors
        scaling_factors = load_scaling_factors(scenario.country_code)

        lazyframe = lazyframe.with_columns(
            pl.when(pl.col("building_use_type_name") == RESIDENTIAL)
            .then(pl.lit(scaling_factors[RESIDENTIAL]))
            .otherwise(pl.lit(scaling_factors[NON_RESIDENTIAL]))
            .alias("calibration_factor")
        )

    return lazyframe


def format_final_df(lazyframe: pl.LazyFrame, full_output: bool) -> pl.LazyFrame:
    """This function formats the DataFrame in the correct order for storage."""
    # Selection
    cols = (
        [
            "stock_projection_year",
            "building_archetype_code",
            "stock_region_name",
            "country_name",
            "building_use_type_name",
            "building_use_subtype_name",
            "LCS_EN15978",
            "element_class_generic_name",
            "techflow_name_mmg",
            "material_name_JRC_CDW",
            "activity_in_out",
            "stock_activity_type_name",
            "carbon_category",
            "stock_floor_area_Mm2",
            "floor_area_country",
            "amount_material",
        ]
        + BASIC_INDICATORS
        + (OPTIONAL_INDICATORS if full_output else [])
        + (["calibration_factor"] if full_output else [])
        + [
            "population_country",
            "population_archetype",
            "floor_area_archetype",
        ]
    )

    lazyframe = lazyframe.select(cols)

    # Ordering and sorting
    if SORT_OUTPUT:
        lazyframe = lazyframe.sort(
            by=[
                "building_archetype_code",
                "stock_projection_year",
                "LCS_EN15978",
                "activity_in_out",
                "element_class_generic_name",
                "techflow_name_mmg",
                "material_name_JRC_CDW",
                "amount_material",
            ],
            nulls_last=True,
        )

    if full_output:
        lazyframe = lazyframe.rename({"mj_per_m2_building": "Final energy consumption (MJ)"})

    return lazyframe


def round_and_downcast(lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """This function rounds and downcasts a selection of columns."""
    columns = lazyframe.collect_schema().names()
    indicators = [col for col in columns if col.startswith("ind_")]

    downcasts: dict[str, type[pl_types.NumericType]] = {
        "stock_projection_year": pl.UInt16,
        "floor_area_archetype": pl.Float32,
        "floor_area_country": pl.Float32,
        "population_archetype": pl.Int32,
        "population_country": pl.Int32,
        "stock_floor_area_Mm2": pl.Float32,
        "amount_material": pl.Float32,
    } | {col: pl.Float32 for col in indicators}

    rounding_columns = indicators  # + ["stock_floor_area_Mm2", "floor_area_archetype", "floor_area_country"]

    # Convert ind_GWP to MtCO₂
    lazyframe = lazyframe.with_columns(pl.col(indicators) / pl.lit(1_000_000_000))

    lazyframe = lazyframe.with_columns(pl.col(rounding_columns).round_sig_figs(NUM_OUTPUT_DIGITS))
    lazyframe = lazyframe.with_columns(pl.col(col).cast(dtype) for col, dtype in downcasts.items())

    return lazyframe


def generate_emission_export(
    emissions: pl.LazyFrame,
    building_stock: BuildingStock,
    folder: str,
    full_output: bool,
) -> None:
    """This function generates and saves the export file."""
    emissions = generate_first_columns(emissions)

    emissions = drop_unnecessary_columns(emissions)

    emissions = group_code(emissions)

    emissions = generate_final_columns(emissions, building_stock, full_output)

    emissions = format_final_df(emissions, full_output)

    emissions = round_and_downcast(emissions)

    store_emissions_export(emissions, folder)
