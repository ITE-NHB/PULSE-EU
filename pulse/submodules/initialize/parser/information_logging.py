"""
information_logging.py
----------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module logs important information
"""

import contextlib
import polars as pl

from pulse.config import ARCHETYPE_STOCK_DATA_PATH, EMISSION_DATA_PATH
from pulse.config import PARSER_LOG_PATH
from pulse.support.arguments import initializeArgs
from pulse.support.defines import (
    B_STAGES,
    CON_STAGES,
    CONSTRUCTION_TYPES,
    COUNTRY_CODE_LIST,
    COUNTRY_CODE_TO_COUNTRY,
    DEM_STAGES,
    HEATING_TYPES,
    NEW_CON_EPOCH,
    REF_STAGES,
    REFURBISHMENT_TYPES,
    RESIDENTIAL_TYPOLOGIES,
    TYPOLOGIES,
    TYPOLOGIES_ORDERED,
)
from pulse.activities.code import (
    code_is_demolishable,
    code_is_residential,
    get_construction_type_from_code,
    get_epoch_end_from_code,
    get_epoch_start_from_code,
    get_heating_type_from_code,
    get_refurb_type_from_code,
    get_typology_from_code,
)


# Stock


def log_bad_codes(stock_df: pl.DataFrame) -> None:
    """Checks if all codes are valid"""
    relevant_cols = stock_df.select("archetype_name")

    for (code,) in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."

        valid_code = len(code) == 29

        valid_code &= code[17:20] in ("EXB", "REF", "NEW")
        valid_code &= get_typology_from_code(code) in TYPOLOGIES
        valid_code &= get_refurb_type_from_code(code) in REFURBISHMENT_TYPES
        valid_code &= code[22] == "0"
        valid_code &= get_construction_type_from_code(code) in CONSTRUCTION_TYPES
        valid_code &= get_heating_type_from_code(code) in HEATING_TYPES
        valid_code &= get_epoch_start_from_code(code) >= 1850
        valid_code &= get_epoch_end_from_code(code) <= NEW_CON_EPOCH[1]

        if not valid_code:
            print(f"    !! Invalid Code: {code} !!")


def log_bad_number_of_buildings(stock_df: pl.DataFrame) -> None:
    """Checks if the number of buildings is good."""
    relevant_cols = stock_df.select("archetype_name", "number of buildings")

    for code, num_buildings in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."
        assert isinstance(num_buildings, float), f"Number of buildings {num_buildings} is invalid for {code}."

        match code[17:20]:
            case "EXB":
                if num_buildings <= 0:
                    print(f"    Number of buildings for {code} is {num_buildings}.")
            case "REF":
                if num_buildings != 0:
                    print(f"    Number of buildings for {code} is {num_buildings}.")
            case "NEW":
                if num_buildings != 0:
                    print(f"    Number of buildings for {code} is {num_buildings}.")
            case _:
                raise KeyError(code[17:20])


def log_bad_useful_floor_area(stock_df: pl.DataFrame) -> None:
    """Checks if the useful floor area is good."""
    relevant_cols = stock_df.select("archetype_name", "reference building useful floor area")

    for code, floor_area in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."
        assert isinstance(floor_area, float), f"Floor area {floor_area} is invalid for {code}."

        if floor_area <= 0:
            print(f"    Useful floor area for {code} is {floor_area}.")


def log_bad_number_of_users(stock_df: pl.DataFrame) -> None:
    """Checks if the number of users is good."""
    relevant_cols = stock_df.select("archetype_name", "number of users")

    for code, number_of_users in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."
        assert isinstance(number_of_users, float), f"Number of users {number_of_users} is invalid for {code}."

        if get_typology_from_code(code) in RESIDENTIAL_TYPOLOGIES:
            if number_of_users <= 0:
                print(f"    Number of users for {code} is {number_of_users}.")
        elif number_of_users != 0:
            print(f"    Number of users for {code} is {number_of_users}.")


def log_bad_occupancy(stock_df: pl.DataFrame) -> None:
    """Checks if the occupancy is good."""
    relevant_cols = stock_df.select("archetype_name", "occupied", "vacant", "secondary dwellings/units and others")

    for code, occupied, vacant, secondary in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."

        if code[17:20] == "NEW":
            if not (occupied is None and vacant is None and secondary is None):
                print(f"    Occupancy data for {code} is {(occupied, vacant, secondary)}.")
        else:
            assert (
                isinstance(occupied, float) and isinstance(vacant, float) and isinstance(secondary, float)
            ), f"Occupancy data for {code} is invalid: {(occupied, vacant, secondary)}."
            occupancy = occupied / (occupied + vacant + secondary)

            if (not code_is_residential(code)) and secondary != 0:
                print(f"    Secondary dwellings for {code} are not 0.")

            if not 0.1 <= occupancy:
                print(f"    Occupancy for {code} is {occupancy*100:.3f}%.")


def log_bad_heating_system_shares(stock_df: pl.DataFrame) -> None:
    """Checks if the heating systems add up to 1."""
    # Heating system distributions
    hs_distributions: dict[str, dict[str, float]] = {}

    relevant_cols = stock_df.select("archetype_name", "HVAC concept - system share")

    for code, hs_share in relevant_cols.iter_rows():
        assert isinstance(code, str), f"{code} is not a string."
        assert isinstance(hs_share, float), f"Heating System share {hs_share} for {code} is not a float."

        relevant_code = code[:-3]
        hs = code[-2:]

        hs_distributions.setdefault(relevant_code, {})[hs] = hs_share

        if hs_share < 0:
            print(f"    Heating System share may NEVER be negative, but is {hs_share} for {code}!")

        if hs_share == 0:
            print(f"    Heating System share of {code} is {hs_share}.")

    for short_code in sorted(hs_distributions.keys()):
        summed = round(sum(hs_distributions[short_code].values()), 10)
        if summed != 1:
            print(f"    Heating system share for {short_code} adds to {summed}.")


def log_missing_ref_types(df: pl.DataFrame, country: str) -> None:
    """This function logs the missing refurbishment types."""
    ref_types: list[str] = (
        df.select("archetype_name")
        .filter(pl.col("archetype_name").str.contains("REF", literal=True))
        .to_series()
        .to_list()
    )

    for typology in TYPOLOGIES_ORDERED:
        typology_relevant = [code for code in ref_types if typology in code]

        if len(typology_relevant) == 0:
            print(f"    No refurbishments found for {country}-{typology}-****-****-REF-**-**-**")
            continue

        for ref_type in ["01", "02", "03"]:
            relevant = [code for code in typology_relevant if code.endswith(f"{ref_type}-{ref_type}")]

            if len(relevant) == 0:
                name = f"{country}-{typology}-****-****-REF-00-{ref_type}-{ref_type}"
                print(f"    No refurbishments found for {name}")


# Emissions


def log_missing_emission_values(emissions_df: pl.LazyFrame) -> None:
    """This function logs values that are missing in the emission data."""
    null_counts = emissions_df.select(pl.exclude("amount_material_kg_per_building")).null_count().collect().to_dict()
    null_counts |= (
        emissions_df.filter(
            pl.col("amount_material_kg_per_building").is_null() & (pl.col("material_name_JRC_CDW") != "Energy")
        )
        .select("amount_material_kg_per_building")
        .null_count()
        .collect()
        .to_dict()
    )

    for column, null_count in null_counts.items():
        null_count_value = null_count.item()
        assert isinstance(null_count_value, int)

        if null_count_value > 0:
            print(f"    {column} contains {null_count_value} missing {"value" if null_count_value == 1 else "values"}")


def log_undefined_material_names(emissions_df: pl.LazyFrame) -> None:
    """This function logs values that are missing in the emission data."""
    valid_values = [
        "Concrete",
        "Ceramics",
        "Plastic",
        "Other Construction Materials",
        "Sand",
        "Steel",
        "Insulation",
        "Glass",
        "Paint and Glue",
        "Brick",
        "Gypsum",
        "Wood",
        "Aluminium",
        "Other Metal",
        "Electronics",
        "Process",
        "Energy",
        "Copper",
        "Cleaning",
    ]
    # invalid = 'Undefined'

    invalid_counts = (
        emissions_df.select("material_name_JRC_CDW")
        .with_columns(count=pl.lit(1))
        .filter(~pl.col("material_name_JRC_CDW").is_in(valid_values))
        .group_by("material_name_JRC_CDW")
        .sum()
        .collect()
        .to_dicts()
    )

    for invalid_count_pair in invalid_counts:
        value = invalid_count_pair["material_name_JRC_CDW"]
        count = invalid_count_pair["count"]

        assert isinstance(value, str)
        assert isinstance(count, int)

        print(f"    material_name_JRC_CD contains an invalid type: '{value}' x{count}")


def log_bad_energy_data(emissions_df: pl.LazyFrame) -> None:
    """This function logs values that are missing in the emission data."""

    invalid_energy_counts = len(
        emissions_df.select("mj_per_m2_building", "activity_in_out")
        .filter((pl.col("activity_in_out") != "ENERGY_IN") & (pl.col("mj_per_m2_building") != 0))
        .collect()
    )

    if invalid_energy_counts:
        print(
            f"    {invalid_energy_counts} rows in mj_per_m2_building contain data "
            + "while activity_in_out is not ENERGY_IN."
        )


# Stock & Emissions


def log_missing_archetype_emissions(stock_df: pl.DataFrame, emissions_df: pl.LazyFrame) -> None:
    """This function logs the archetypes that are missing in the emission data."""

    def check_emission_data_exists(
        emissions: pl.DataFrame, codes: list[str], stages: list[str]
    ) -> dict[str, list[str]]:
        missing_codes: dict[str, list[str]] = {}

        for stage in stages:
            rel = emissions.filter(pl.col("LCS_EN15978").eq(stage))

            for code in codes:
                if rel.filter(pl.col("building_archetype_code").eq(code)).is_empty():
                    missing_codes.setdefault(code, []).append(stage)

        return missing_codes

    archetype_data = stock_df.select("archetype_name")
    codes = pl.Series(archetype_data).to_list()

    lf = emissions_df.select("building_archetype_code", "LCS_EN15978")
    lf = lf.filter(pl.struct(pl.all()).is_first_distinct())
    emissions = lf.collect()

    ref_codes = list(filter(lambda code: str(code).find("REF") != -1, codes))
    new_codes = list(filter(lambda code: str(code).find("NEW") != -1, codes))
    not_new_codes = list(filter(lambda code: str(code).find("NEW") == -1, codes))
    dem_codes = list(filter(code_is_demolishable, codes))

    all_missing_codes: dict[str, list[str]] = {}

    for code, stages in check_emission_data_exists(emissions, not_new_codes, B_STAGES).items():
        code_list = all_missing_codes.setdefault(code, [])
        code_list += stages
    for code, stages in check_emission_data_exists(emissions, new_codes, [s for s in B_STAGES if s != "B4"]).items():
        code_list = all_missing_codes.setdefault(code, [])
        code_list += stages
    for code, stages in check_emission_data_exists(emissions, dem_codes, DEM_STAGES).items():
        code_list = all_missing_codes.setdefault(code, [])
        code_list += stages
    for code, stages in check_emission_data_exists(emissions, new_codes, CON_STAGES).items():
        code_list = all_missing_codes.setdefault(code, [])
        code_list += stages
    for code, stages in check_emission_data_exists(emissions, ref_codes, REF_STAGES).items():
        code_list = all_missing_codes.setdefault(code, [])
        code_list += stages

    for code in sorted(all_missing_codes.keys()):
        print(f"    {code} is missing data for {sorted(all_missing_codes[code])}")


#


def log_problems() -> None:
    """This function calls all the logging functions."""
    args = initializeArgs

    with contextlib.redirect_stdout(open(PARSER_LOG_PATH.format(args.output_folder), mode="w", encoding="UTF-8")):
        for country in COUNTRY_CODE_LIST:
            print(f"Checking {COUNTRY_CODE_TO_COUNTRY[country]}")

            stock_df = pl.read_parquet(ARCHETYPE_STOCK_DATA_PATH.format(country.lower()))
            emissions_df = pl.read_parquet(EMISSION_DATA_PATH.format(country.lower())).lazy()

            log_bad_codes(stock_df)

            log_bad_number_of_buildings(stock_df)
            log_bad_useful_floor_area(stock_df)
            log_bad_number_of_users(stock_df)
            log_bad_occupancy(stock_df)
            log_bad_heating_system_shares(stock_df)

            log_missing_ref_types(stock_df, country)

            log_missing_emission_values(emissions_df)
            log_undefined_material_names(emissions_df)
            log_bad_energy_data(emissions_df)

            log_missing_archetype_emissions(stock_df, emissions_df)
