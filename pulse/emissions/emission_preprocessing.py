"""
emission_preprocessing.py
-------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains the class for the PreprocessingParameter and PreprocessingFilter.
"""

import polars as pl

from pulse.activities import Scenario
from pulse.config import SCENARIO_PATH
from pulse.support.debug_flags import CHECK_FILTERS_GET_DATA
from pulse.support.defines import BASIC_INDICATORS, OPTIONAL_INDICATORS
from pulse.support.file_interaction import load_dict_from_pickle
from pulse.support.logging import log_warning
from pulse.support.ui import format_list


FILTER_COLUMNS = [
    "stock_activity_type_code",
    "building_use_subtype_code",
    "element_class_generic_name",
    "material_name_JRC_CDW",
    "LCS_EN15978",
    "activity_in_out",
    "techflow_name_mmg",
]

FUZZY_FILTERS = ["techflow_name_mmg"]


class PreprocessingParameter:
    """Parameters defining the emission reductions"""

    name: str
    col_name: str
    condition: pl.Expr
    affected_columns: list[str]
    yearly_reduction_values: dict[int, float]

    def __init__(self, scenario: Scenario, name: str, data: dict[str, object], full_output: bool):
        """Unknown method"""
        self.name = name
        self.col_name = f"preprocessing_{name}"

        self.condition = self.__calculate_expression(data)

        available_indicators = BASIC_INDICATORS + (OPTIONAL_INDICATORS if full_output else [])

        indicator_data = data["ind"]
        assert isinstance(
            indicator_data, (str, list)
        ), f"Indicator data must be a str or list! type({indicator_data}) = {type(indicator_data)}"

        if indicator_data == "*":
            self.affected_columns = available_indicators
        else:
            assert isinstance(
                indicator_data, list
            ), f"Indicator data must be a list! type({indicator_data}) = {type(indicator_data)}"

            self.affected_columns = []

            for ind_col in indicator_data:
                assert isinstance(ind_col, str), f"Indicator column must be a str! type({ind_col}) = {type(ind_col)}"
                assert ind_col in available_indicators, f"Don't apply filters to invalid Indicator columns: {ind_col}"

                self.affected_columns.append(ind_col)

        tmp = data["amount_material_kg_per_building"]
        assert isinstance(tmp, bool), f"amount_material_kg_per_building must be a bool! type({tmp}) = {type(tmp)}"

        if tmp:
            self.affected_columns.append("amount_material_kg_per_building")

        self.yearly_reduction_values = {}

        for year in range(scenario.start, scenario.end + 1, scenario.step_size):
            value = data[str(year)]
            assert isinstance(value, float)

            self.yearly_reduction_values[year] = value

    def __calculate_expression(self, data: dict[str, object]) -> pl.Expr:
        """This function calculates the sub expression for a preprocessing sub-filter."""
        condition = pl.lit(True)

        for filter_column in FILTER_COLUMNS:
            allowed_values = data[filter_column]

            assert isinstance(
                allowed_values, (str, list)
            ), f"Value must be a str or list! type({allowed_values}) = {type(allowed_values)}"

            if allowed_values == "*":
                # Applies to all, no filter necessary
                continue

            assert isinstance(
                allowed_values, list
            ), f"Value must be an instance of list! type({allowed_values}) = {type(allowed_values)}"

            if filter_column in FUZZY_FILTERS:
                condition &= pl.col(filter_column).str.contains_any(allowed_values)
            else:
                condition &= pl.col(filter_column).is_in(allowed_values)

        return condition

    def apply(self, lazyframe: pl.LazyFrame, year: int) -> pl.LazyFrame:
        """Applies the parameter to the data."""
        multiplier = 1 - self.yearly_reduction_values[year]

        if multiplier == 1.0:
            return lazyframe

        lazyframe = lazyframe.with_columns(
            pl.when(pl.col(self.col_name)).then(pl.col(col) * multiplier).otherwise(pl.col(col)).alias(col)
            for col in self.affected_columns
        )

        return lazyframe

    def __str__(self) -> str:
        name = self.__class__.__name__
        string = (
            f"{name} CRS {self.name}\n"
            + f"{"-" * len(f"{name} CRS {self.name}")}\n"
            + f"Affected columns: {format_list(self.affected_columns)}"
        )

        return string


def load_parameters(scenario: Scenario, folder: str, full_output: bool) -> list[PreprocessingParameter]:
    """This function loads the Scenario data relevant for emissions."""
    preproc_data: dict[str, object] = load_dict_from_pickle(SCENARIO_PATH.format(folder, scenario.name))

    parameters: list[PreprocessingParameter] = []

    for name, data in preproc_data.items():
        if (not isinstance(data, dict)) or data["implementation"] != "preprocessing":
            continue

        parameters.append(PreprocessingParameter(scenario, name, data, full_output))

    return parameters


def add_preprocessing_columns(lf: pl.LazyFrame, parameters: list[PreprocessingParameter]) -> pl.LazyFrame:
    """Precomputes the filters so they can simply be used as boolean columns in the calculations."""
    for parameter in parameters:
        lf = lf.with_columns(parameter.condition.alias(f"preprocessing_{parameter.name}")).collect().lazy()

    if CHECK_FILTERS_GET_DATA:
        for parameter in parameters:
            if not lf.select(f"preprocessing_{parameter.name}").collect().to_series().any():
                log_warning(f"Found no data matching preprocessing filter {parameter}.")

    return lf


def preprocess_emissions(data: pl.LazyFrame, parameters: list[PreprocessingParameter], year: int) -> pl.LazyFrame:
    """This function gets the preprocessed emission data."""
    lazyframe = data

    for parameter in parameters:
        lazyframe = parameter.apply(lazyframe, year)

    lazyframe = lazyframe.with_columns(
        (pl.col("ind_GWP_Fos") + pl.col("ind_GWP_Bio") + pl.col("ind_GWP_LuLuc")).alias("ind_GWP_Tot")
    )

    return lazyframe
