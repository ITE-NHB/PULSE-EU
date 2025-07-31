"""
scaling.py
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    Scaling factors are calculated for each country for residential and non-residential types. Reference values (B6,\
    2020) are stored in `data/raw/reference_b6_2020.json`. The scaling factors are stored in\
    `data/parsed/scaling_b6.json`.
"""

import os
import polars as pl

from pulse.support.arguments import initializeArgs
from pulse.support.file_interaction import hash_file, load_dict_from_json, write_dict_to_json
from pulse.support.defines import NON_RESIDENTIAL, RESIDENTIAL, RESIDENTIAL_TYPOLOGIES
from pulse.config import (
    ARCHETYPE_EMISSION_RAW_DATA,
    ARCHETYPE_STOCK_DATA_PATH,
    EMISSION_DATA_PATH,
    EMISSION_EXPORT_PATH,
    REFERENCE_B6_2020_PATH,
    SCALING_FACTOR_PATH,
)

SCALING_INDICATOR = "ind_GWP_Tot"


def get_current_scaling_factors(country: str) -> dict[str, float]:
    """Calculates the scaling factor by taking the ratio between the parsed and raw emission data."""
    scaling_condition = pl.col("LCS_EN15978") == "B6"  # & (pl.col("building_archetype_code").str.slice(17, 3) == "EXB")

    raw_emissions = (
        pl.scan_parquet(ARCHETYPE_EMISSION_RAW_DATA.format(country))
        .filter(scaling_condition)
        .with_columns(
            residential=pl.col("building_use_subtype_code").is_in(RESIDENTIAL_TYPOLOGIES),
        )
        .select("residential", SCALING_INDICATOR)
        .group_by("residential")
        .sum()
        .collect()
    )

    parsed_emissions = (
        pl.scan_parquet(EMISSION_DATA_PATH.format(country))
        .filter(scaling_condition)
        .with_columns(
            residential=pl.col("building_use_subtype_code").is_in(RESIDENTIAL_TYPOLOGIES),
        )
        .select("residential", SCALING_INDICATOR)
        .group_by("residential")
        .sum()
        .collect()
    )

    parsed_res = parsed_emissions.filter(pl.col("residential")).select("ind_GWP_Tot")
    parsed_non_res = parsed_emissions.filter(~pl.col("residential")).select("ind_GWP_Tot")

    raw_res = raw_emissions.filter(pl.col("residential")).select("ind_GWP_Tot")
    raw_non_res = raw_emissions.filter(~pl.col("residential")).select("ind_GWP_Tot")

    total_emissions = {
        RESIDENTIAL: parsed_res.item() * 50 / raw_res.item(),
        NON_RESIDENTIAL: parsed_non_res.item() * 50 / raw_non_res.item(),
    }

    return total_emissions


def get_emissions(country: str, folder: str) -> dict[str, float]:
    """Gets the emissions in megatons like."""
    folder = os.path.abspath(os.path.join(folder, "runs", country))
    path = EMISSION_EXPORT_PATH.format(folder)

    scaling_condition = pl.col("LCS_EN15978") == "B6"  # & (pl.col("building_archetype_code").str.slice(17, 3) == "EXB")

    emissions = (
        pl.scan_parquet(path)
        .filter((pl.col("stock_projection_year") == 2020) & scaling_condition)
        .with_columns(
            residential=pl.col("building_use_subtype_name").is_in(RESIDENTIAL_TYPOLOGIES),
        )
        .select("residential", SCALING_INDICATOR)
        .group_by("residential")
        .sum()
        .collect()
    )

    emission_s1_res = emissions.filter(pl.col("residential")).select("ind_GWP_Tot")
    emission_s1_non_res = emissions.filter(~pl.col("residential")).select("ind_GWP_Tot")

    total_emissions = {
        RESIDENTIAL: emission_s1_res.item(),
        NON_RESIDENTIAL: emission_s1_non_res.item(),
    }

    return total_emissions


def get_reference_emissions(country: str) -> dict[str, float]:
    """Gets the reference emissions in megatons like (residential, non_residential)."""
    reference_emissions = load_dict_from_json(REFERENCE_B6_2020_PATH)

    country_emissions: dict[str, dict[str, float]] = reference_emissions[country]

    summed_emissions = {
        RESIDENTIAL: country_emissions["Scope 1"][RESIDENTIAL] + country_emissions["Scope 2"][RESIDENTIAL],
        NON_RESIDENTIAL: country_emissions["Scope 1"][NON_RESIDENTIAL] + country_emissions["Scope 2"][NON_RESIDENTIAL],
    }

    return summed_emissions


def update_scaling_factor(country: str, scaling_factors: dict[str, float]) -> None:
    """Updates the scaling factors in the file by multiplying them by the given number."""
    current_data: dict[str, dict[str, float | str]] = {}

    if os.path.isfile(SCALING_FACTOR_PATH):
        current_data = load_dict_from_json(SCALING_FACTOR_PATH)

    current_data[country] = scaling_factors | {
        "archetype_data hash": "",
        "emission_data hash": "",
    }

    write_dict_to_json(current_data, SCALING_FACTOR_PATH)


def update_hashes(countries: list[str]) -> None:
    """Updates the hashes of the updated countries."""
    if not countries:
        return

    current_data: dict[str, dict[str, float | str]] = load_dict_from_json(SCALING_FACTOR_PATH)

    for country in countries:
        archetype_data_hash = hash_file(ARCHETYPE_STOCK_DATA_PATH.format(country))
        emission_data_hash = hash_file(EMISSION_DATA_PATH.format(country))

        current_data[country]["archetype_data hash"] = archetype_data_hash
        current_data[country]["emission_data hash"] = emission_data_hash

    write_dict_to_json(current_data, SCALING_FACTOR_PATH)


def ensure_base_is_up_to_date(country: str, folder: str) -> None:
    """Checks if the base scenario exists for the given country and is more recent than the emission data."""
    base_emission_path = os.path.join(folder, "runs", country, "emissions.parquet")
    emission_data_path = EMISSION_DATA_PATH.format(country)
    stock_data_path = ARCHETYPE_STOCK_DATA_PATH.format(country)

    if not os.path.exists(emission_data_path):
        raise FileNotFoundError(f"{country} is missing the archetype emissions file.")

    if not os.path.exists(stock_data_path):
        raise FileNotFoundError(f"{country} is missing the archetype stock file.")

    if not os.path.exists(base_emission_path):
        raise FileNotFoundError(f"{country} is missing the BASE emissions file.")

    # Check if the emissions file is older than the emission data file, which means that the base scenario might
    # be based on a different emission data file
    if os.path.getmtime(base_emission_path) < os.path.getmtime(emission_data_path):
        raise ValueError(f"{country} BASE Scenario is outdated.")


def recalculate_scaling_factors(countries: list[str]) -> list[str]:
    """Recalculates the scaling factors and updates them for the given country."""
    updated_countries: list[str] = []

    output_folder = initializeArgs.output_folder

    for country in countries:
        ensure_base_is_up_to_date(country, output_folder)

        current_scaling = get_current_scaling_factors(country)

        reference_emissions = get_reference_emissions(country)
        base_emissions = get_emissions(country, output_folder)

        assert all(factor > 0 for factor in base_emissions.values()), f"Base emissions are not valid for {country}!"
        assert all(factor > 0 for factor in current_scaling.values()), f"Current scalings are not valid for {country}!"

        scaling_factors = {
            RESIDENTIAL: (
                current_scaling[RESIDENTIAL] * reference_emissions[RESIDENTIAL] / base_emissions[RESIDENTIAL]
            ),
            NON_RESIDENTIAL: (
                current_scaling[NON_RESIDENTIAL]
                * reference_emissions[NON_RESIDENTIAL]
                / base_emissions[NON_RESIDENTIAL]
            ),
        }

        update_scaling_factor(country, scaling_factors)
        updated_countries.append(country)

    return updated_countries
