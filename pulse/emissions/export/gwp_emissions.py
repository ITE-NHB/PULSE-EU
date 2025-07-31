"""
gwp_emissions.py
----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the gwp emission calculations
"""

import math
import polars as pl

from pulse.activities import Scenario
from pulse.support.defines import RESIDENTIAL_TYPOLOGIES, TYPOLOGIES

from .defines import COMMERCIAL, NUM_OUTPUT_DIGITS, RES_AND_COMM, RESIDENTIAL, STAGE_TO_VARIABLE

SELECTED_INDICATOR = "ind_GWP_Tot"

GET_TYPOLOGY_SPECIFIC_EMISSIONS = False


# Function to aggregate GWP[total] by year for each stage
def aggregate_gwp(df: pl.LazyFrame, stages: set[str]) -> dict[str, dict[int, tuple[float, float]]]:
    """This function aggregates the data for each stage."""
    grouped = (
        df.with_columns(pl.col("building_archetype_code").str.slice(3, 3).is_in(RESIDENTIAL_TYPOLOGIES).alias("is_res"))
        .select("is_res", "Year", "LCS_EN15978", SELECTED_INDICATOR)
        .group_by(
            "is_res",
            "LCS_EN15978",
            "Year",
        )
        .sum()
        .collect()
    )

    result: dict[str, dict[int, tuple[float, float]]] = {}

    for stage in stages:
        filtered = grouped.filter(pl.col("LCS_EN15978") == stage)

        data: dict[int, tuple[float, float]] = {}
        for year, is_res, val in filtered.select(["Year", "is_res", SELECTED_INDICATOR]).iter_rows():
            assert isinstance(year, int) and isinstance(val, float)

            data.setdefault(year, (0.0, 0.0))

            data[year] = (val, data[year][1]) if is_res else (data[year][0], val)

        result[stage] = data

    return result


# Function to aggregate GWP[total] by year for each stage
def aggregate_gwp_with_separate_typologies(
    df: pl.LazyFrame, stages: set[str]
) -> dict[str, dict[str, dict[int, float]]]:
    """This function aggregates the data for each stage."""
    df = df.select(
        [pl.col("building_archetype_code").str.slice(3, 3).alias("typology"), "Year", "LCS_EN15978", SELECTED_INDICATOR]
    )
    grouped = df.group_by(["typology", "LCS_EN15978", "Year"]).sum().collect()

    result: dict[str, dict[str, dict[int, float]]] = {}

    for stage in stages:
        filtered = grouped.filter(pl.col("LCS_EN15978").eq(stage))
        result[stage] = {}

        for typology in TYPOLOGIES:
            t_filtered = filtered.filter(pl.col("typology") == (typology))

            data: dict[int, float] = {}
            for year, val in t_filtered.select(["Year", SELECTED_INDICATOR]).iter_rows():
                assert isinstance(year, int) and isinstance(val, float)
                data[year] = val

            result[stage][typology] = data

    return result


def calculate_gwp(scenario: Scenario, emissions: pl.LazyFrame, is_generation_run: bool) -> pl.DataFrame:
    """Calculates and aggregates GWP emissions by life cycle stage and year."""

    def rounding_func(num: float) -> float:
        if num == 0 or math.isnan(num):
            return num
        return round(num, (NUM_OUTPUT_DIGITS - 1) - math.floor(math.log10(abs(num))))

    # Define the stages for each category
    stages: set[str] = {"A1-3", "A4", "A5", "B2", "B4", "B5", "B6", "C1", "C2", "C3", "C4"}

    # assert emission_data["LCS_EN15978"].isin(stages).all(), "Invalid stage found!"

    df_list: list[pl.DataFrame] = []

    new_data: dict[str, str | float]

    if not GET_TYPOLOGY_SPECIFIC_EMISSIONS:
        # Aggregate GWP[total] for each category
        gwp_summary = aggregate_gwp(emissions, stages)

        for stage, data in gwp_summary.items():
            for start, end, use in ((0, 2, RES_AND_COMM), (0, 1, RESIDENTIAL), (1, 2, COMMERCIAL)):
                new_data = {
                    "Variable": f"{SELECTED_INDICATOR}|{STAGE_TO_VARIABLE[stage]}|{use}",
                    "Unit": "Mt CO₂",
                }

                for year in range(scenario.start, scenario.end + 1, scenario.step_size):
                    value = sum(data.get(year, (0.0, 0.0))[start:end])
                    new_data[str(year)] = rounding_func(value / 1_000_000_000)

                    if is_generation_run:
                        break

                df_list.append(pl.DataFrame(new_data, orient="row"))
    else:
        # Aggregate GWP[total] for each category
        t_gwp_summary = aggregate_gwp_with_separate_typologies(emissions, stages)

        for stage, stage_data in t_gwp_summary.items():
            for typology, t_data in stage_data.items():
                new_data = {
                    "Variable": f"{SELECTED_INDICATOR}|{STAGE_TO_VARIABLE[stage]}|{typology}",
                    "Unit": "Mt CO₂",
                }

                for year in range(scenario.start, scenario.end + 1, scenario.step_size):
                    new_data[str(year)] = rounding_func(t_data.get(year, 0.0) / 1_000_000_000)

                    if is_generation_run:
                        break

                df_list.append(pl.DataFrame(new_data, orient="row"))

    final_df = pl.concat(df_list)
    final_df = final_df.sort("Variable")

    return final_df
