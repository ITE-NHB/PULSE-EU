"""
emission_data.py
----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The emission data parser.
"""

import polars as pl

from pulse.config import ARCHETYPE_EMISSION_RAW_DATA, EMISSION_DATA_PATH
from pulse.support.defines import (
    B_STAGES,
    BASIC_INDICATORS,
    NON_RESIDENTIAL,
    OPTIONAL_INDICATORS,
    RESIDENTIAL,
    RESIDENTIAL_TYPOLOGIES,
)
from pulse.support.file_interaction import load_scaling_factors


EMISSIONS_INCLUDED_COLUMNS = [
    "building_archetype_code",
    "element_class_generic_name",
    "techflow_name_mmg",
    "material_name_JRC_CDW",
    "amount_material_kg_per_building",
    "activity_in_out",
    "LCS_EN15978",
    "stock_activity_type_code",
    "building_use_subtype_code",
]


def apply_b_preprocessing(lf: pl.LazyFrame, country: str, indicators: list[str]) -> pl.LazyFrame:
    """
    This processes B stage emissions, so that the output file contains the average yearly values for each archetype.
    """
    # Column definitions
    group_by_cols = [
        "building_archetype_code",
        "element_class_generic_name",
        "techflow_name_mmg",
        "material_name_JRC_CDW",
        "activity_in_out",
        "LCS_EN15978",
        "stock_activity_type_code",
        "building_use_subtype_code",
    ]

    agg_cols = indicators + ["amount_material_kg_per_building"]

    columns = lf.collect_schema().names()
    used = group_by_cols + agg_cols
    assert len(used) == len(set(used)) == len(columns)

    lf = lf.group_by(group_by_cols).sum().select(columns)

    # Aggregate B stage emissions
    b_emissions = (
        lf.filter(pl.col("LCS_EN15978").is_in(B_STAGES))
        .with_columns(pl.col(ind_col) / 50 for ind_col in agg_cols)
        .select(columns)
    )

    # remove all b_emissions from emissions
    lf = lf.filter(~(pl.col("LCS_EN15978").is_in(B_STAGES)))

    # add b_emissions_aggregated to emissions
    lf = pl.concat([lf, b_emissions], how="vertical")

    # Scale B6 emissions
    scaling_factors = load_scaling_factors(country)
    assert all(scaling_factor > 0 for scaling_factor in scaling_factors.values())

    condition_b6 = pl.col("LCS_EN15978") == "B6"
    condition_res = pl.col("building_use_subtype_code").is_in(RESIDENTIAL_TYPOLOGIES)

    lf = lf.with_columns(
        [
            # Residential factor
            pl.when(condition_b6 & condition_res).then(pl.col(ind_col) * scaling_factors[RESIDENTIAL])
            # Non-Residential factor
            .when(condition_b6 & ~condition_res).then(pl.col(ind_col) * scaling_factors[NON_RESIDENTIAL])
            # non
            .otherwise(pl.col(ind_col)).alias(ind_col)
            for ind_col in indicators
        ]
    )

    return lf


def fix_data_discrepancies(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Lalala, lululu..."""
    lf = lf.with_columns(
        (
            pl.col("building_archetype_code")
            .str.replace("2021", "2019", literal=True)
            .str.replace("2022-2025", "2020-2050", literal=True)
        ),
        pl.col("activity_in_out").replace("Energy in", "ENERGY_IN"),
        pl.col("mj_per_m2_building").fill_null(0),
    )

    return lf


def parse_emissions(countries: list[str]) -> None:
    """Main emission data parsing function."""
    print("[" + " " * len(countries) + "]", end="\033[2G", flush=True)

    for country in countries:
        emissions = pl.scan_parquet(ARCHETYPE_EMISSION_RAW_DATA.format(country.lower()))

        emissions = emissions.rename({"MJ_PER_M2_BUILDING": "mj_per_m2_building"})

        indicators = BASIC_INDICATORS + OPTIONAL_INDICATORS

        emissions = emissions.select(EMISSIONS_INCLUDED_COLUMNS + indicators)

        emissions = apply_b_preprocessing(emissions, country, indicators)

        emissions = fix_data_discrepancies(emissions)

        emissions = emissions.sort(pl.all())

        emissions.collect().write_parquet(EMISSION_DATA_PATH.format(country.lower()))

        print("#", end="", flush=True)

    print()
