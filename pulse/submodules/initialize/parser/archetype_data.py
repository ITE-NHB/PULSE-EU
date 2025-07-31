"""
archetype_data.py
-----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The archetype data parser.
"""

import json
import polars as pl

from pulse.config import (
    ARCHETYPE_EMISSION_RAW_DATA,
    ARCHETYPE_STOCK_DATA_PATH,
    ARCHETYPE_STOCK_RAW_DATA,
    POPULATION_STATISTIC_PATH,
    POPULATION_DISTRIBUTION_RAW_DATA,
)
from pulse.support.defines import CONSTRUCTION_TYPES, NEW_CON_EPOCH, RESIDENTIAL_TYPOLOGIES

ARCHETYPE_INCLUDED_COLUMNS = [
    "archetype_name",
    "number of buildings",
    "reference building useful floor area",
    "number of users",
    "occupied",
    "vacant",
    "secondary dwellings/units and others",
    "HVAC concept - system share",
]

YEAR_TO_CALC = 2019


def fix_data_discrepancies(df: pl.DataFrame) -> pl.DataFrame:
    """There never was a complaint in this function docstring in a previous commit, i promise"""

    df = df.with_columns(
        (pl.col("Useful floor area") / pl.col("reference building useful floor area")).alias("number of buildings"),
    )

    df = df.with_columns(
        # Fix the archetype names to specify the correct range
        pl.col("archetype_name")
        .str.replace("2021", "2020", literal=True)
        .str.replace("2022-2025", "2020-2050", literal=True)
    )

    df = df.with_columns(
        pl.when(
            pl.col("number of buildings").is_null()
            & (pl.col("archetype_name").str.slice(12, 4) == str(NEW_CON_EPOCH[1]))
        )
        .then(pl.lit(0))
        .otherwise("number of buildings")
        .alias("number of buildings"),
    )

    assert df.select("number of buildings").null_count().item(0, 0) == 0

    df = df.filter((pl.col("number of buildings") > 0) | (pl.col("archetype_name").str.slice(17, 3) != "EXB"))

    return df


def cut_off_extra_years(df: pl.DataFrame, year: int) -> pl.DataFrame:
    """This function cuts off the extra years from the data."""
    df = df.with_columns(
        epoch_start=pl.col("archetype_name").str.slice(7, 4).cast(pl.Int16),
        epoch_end=pl.col("archetype_name").str.slice(12, 4).cast(pl.Int16),
    )

    df = df.with_columns(
        percentage=((pl.lit(year) - pl.col("epoch_start")) / (pl.col("epoch_end") - pl.col("epoch_start"))).clip(0, 1)
    ).with_columns(
        pl.when(pl.col("epoch_start") != NEW_CON_EPOCH[0])
        .then((pl.col("number of buildings") * pl.col("percentage")).alias("number of buildings"))
        .otherwise(pl.col("number of buildings")),
        pl.when(pl.col("epoch_start") != NEW_CON_EPOCH[0])
        .then((pl.col("Useful floor area") * pl.col("percentage")).alias("Useful floor area"))
        .otherwise(pl.col("Useful floor area")),
        pl.when((pl.col("epoch_start") != NEW_CON_EPOCH[0]) & (pl.col("epoch_end") == 2020))
        .then(
            (
                pl.col("archetype_name").str.slice(0, 12) + pl.lit(str(year)) + pl.col("archetype_name").str.slice(16)
            ).alias("archetype_name")
        )
        .otherwise(pl.col("archetype_name")),
    )
    df = df.drop("epoch_start", "epoch_end", "percentage")

    return df


def recalculate_capacities(df: pl.DataFrame, country: str) -> pl.DataFrame:
    """This function recalculates the capacities of the buildings."""
    with open(POPULATION_STATISTIC_PATH.format(country), encoding="UTF-8") as file:
        population: int = json.load(file)[str(YEAR_TO_CALC)]

    typology_pop = {
        typology: percentage / 100 * population
        for typology, percentage in (
            pl.read_csv(POPULATION_DISTRIBUTION_RAW_DATA).row(by_predicate=pl.col("Country") == country, named=True)
        ).items()
        if typology in RESIDENTIAL_TYPOLOGIES
    }

    stock = df.filter(
        pl.col("archetype_name").str.contains_any(RESIDENTIAL_TYPOLOGIES)
        & pl.col("archetype_name").str.contains("EXB", literal=True)
    )

    stock = stock.with_columns(
        pl.col("archetype_name").str.slice(3, 3).alias("typology"),
    )

    stock = stock.join(
        stock.group_by("typology")
        .agg(pl.col("number of dwellings/units").sum().alias("total dwellings in typology"))
        .with_columns(
            pl.when(pl.col("typology").is_in(RESIDENTIAL_TYPOLOGIES))
            .then(pl.col("total dwellings in typology"))
            .otherwise(pl.lit(0))
        ),
        on="typology",
    )

    stock = stock.with_columns_seq(
        (
            pl.col("number of dwellings/units")
            / pl.col("total dwellings in typology")
            * pl.col("typology").replace_strict(typology_pop, return_dtype=pl.Float64)
            / pl.col("number of buildings")
            / (
                pl.col("occupied")
                / (pl.col("occupied") + pl.col("vacant") + pl.col("secondary dwellings/units and others"))
            )
        ).round(10)
        # .cast(pl.Int16)
        .alias("new_capacity")
    )

    mapping_dict: dict[str, float] = dict(zip(stock["archetype_name"].to_list(), stock["new_capacity"].to_list()))

    short_code_mapping: dict[str, float] = {}

    for code, capacity in mapping_dict.items():
        short_code = code[:11]

        if short_code not in short_code_mapping:
            short_code_mapping[short_code] = capacity
        else:
            assert short_code_mapping[short_code] == capacity

    df = df.with_columns(
        pl.col("archetype_name")
        .str.replace(str(NEW_CON_EPOCH[0]), "2011", literal=True)
        .str.slice(0, 11)
        .replace_strict(short_code_mapping, default=0, return_dtype=pl.Float64)
        .alias("number of users")
    )

    return df


def add_construction_types(df: pl.DataFrame) -> pl.DataFrame:
    """This function adds the construction types to the codes."""
    constructions = df.filter(pl.col("archetype_name").str.contains("NEW", literal=True))

    relevant_c_types = CONSTRUCTION_TYPES.copy()
    relevant_c_types.remove("00")

    for c_type in relevant_c_types:
        # AT-SFH-2011-2021-EXB-00-00-00
        df = df.vstack(
            constructions.with_columns(
                pl.col("archetype_name").str.replace("-00-", f"-{c_type}-", literal=True).alias("archetype_name")
            )
        )

    df = df.sort("archetype_name")

    assert df.select("archetype_name").is_unique().all() is True

    return df


def add_refurbishment_types(df: pl.DataFrame, ref_codes: list[str]) -> pl.DataFrame:
    """This function adds the refurbishment types to the codes."""
    refurbishments = df.filter(
        pl.col("archetype_name").str.contains("EXB", literal=True)
        & (pl.col("archetype_name").str.slice(7, 4) != "2011")
    )

    base_ref_codes = [f"{code[:16]}-EXB-00-00-01" for code in ref_codes]
    base_ref_codes = [
        "LU-ABL-2000-2010-EXB-00-00-02" if c == "LU-ABL-2000-2010-EXB-00-00-01" else c for c in base_ref_codes
    ]

    base_codes = pl.DataFrame({"archetype_name": base_ref_codes, "ref_name": ref_codes})

    ref_df = refurbishments.join(base_codes, on="archetype_name", how="right")

    ref_df = ref_df.drop("archetype_name")
    ref_df = ref_df.rename({"ref_name": "archetype_name"})

    ref_df = ref_df.select(df.columns)

    ref_df = ref_df.with_columns(
        pl.lit(0.0, dtype=pl.Float64).alias("number of buildings"),
        pl.lit(1.0, dtype=pl.Float64).alias("HVAC concept - system share"),
    )

    # AT-SFH-2011-2021-EXB-00-00-00
    df = df.vstack(ref_df)

    df = df.sort("archetype_name")

    assert df.select("archetype_name").is_unique().all()

    return df


def parse_archetype_data(countries: list[str]) -> None:
    """Main archetype parsing function."""

    for country in countries:
        # If input file does not exist, exit
        df = pl.read_parquet(ARCHETYPE_STOCK_RAW_DATA.format(country.lower()))

        df = fix_data_discrepancies(df)

        df = cut_off_extra_years(df, YEAR_TO_CALC)

        df = recalculate_capacities(df, country)

        df = df.select(ARCHETYPE_INCLUDED_COLUMNS)

        ref_codes = (
            pl.scan_parquet(ARCHETYPE_EMISSION_RAW_DATA.format(country.lower()))
            .select("building_archetype_code")
            .unique("building_archetype_code")
            .filter(
                pl.col("building_archetype_code").str.contains("REF", literal=True)
                & ~(pl.col("building_archetype_code").str.slice(7, 4).is_in(["2011", "2022"]))
            )
            .with_columns(pl.col("building_archetype_code").str.replace("2021", "2019", literal=True))
            .collect()
            .to_series()
            .to_list()
        )

        df = add_refurbishment_types(df, ref_codes)

        df = df.sort(pl.all())

        path = ARCHETYPE_STOCK_DATA_PATH.format(country.lower())

        df.write_parquet(path)
