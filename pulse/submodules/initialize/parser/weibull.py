"""
weibull.py
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The weibull data generator.
"""

# ----------------------------------------------------------------------------------------------------------------
# Functions
# ----------------------------------------------------------------------------------------------------------------
from math import e

from pulse.config import WEIBULL_PARAMETERS_RAW_DATA, WEIBULL_STATISTIC_PATH
from pulse.support.defines import POST_WAR, PRE_WAR, RESIDENTIAL, WeibullDataType
from pulse.support.file_interaction import load_dict_from_json, write_dict_to_json


def calc_weibull(k: float, lam: float, age_range: tuple[int, int] = (1, 201)) -> list[float]:
    """
    This function calculates the Weibull values based on the input variables across an age range.
    """
    output = [0.0]

    for x in range(age_range[0], age_range[1]):

        weibull = k / lam * (x / lam) ** (k - 1) * e ** -((x / lam) ** k)

        output.append(weibull)
    return output


def get_weibull_parameters(country: str, typology: str, pre_war: bool) -> tuple[float, float]:
    """This function gets the Weibull parameters for a country with the specs specified.
    If it does not exist, returns the default value for that country"""
    weibull_parameters: WeibullDataType = load_dict_from_json(WEIBULL_PARAMETERS_RAW_DATA)

    assert country in weibull_parameters, f"{country} is not in weibull_parameters: {weibull_parameters.keys()}"

    selected_country = weibull_parameters[country]
    post_or_pre_war = PRE_WAR if pre_war else POST_WAR

    data = None

    if typology in selected_country:
        selected_typology = selected_country[typology]
        assert isinstance(selected_typology, dict)

        if post_or_pre_war in selected_typology:
            data = selected_typology[post_or_pre_war]

    if data is None:
        sel = selected_country[post_or_pre_war]
        assert isinstance(sel, list), f"Expected list for {country} {typology} {post_or_pre_war}, got {type(sel)}"
        data = sel

    assert isinstance(data, list)

    return data[0], data[1]


def get_weibull_for_country(country_code: str) -> dict[str, dict[str, list[float]]]:
    """This gets the weibull data for a selected country."""
    result = {
        RESIDENTIAL: {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, RESIDENTIAL, pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, RESIDENTIAL, pre_war=True)),
        },
        "EDU": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "EDU", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "EDU", pre_war=True)),
        },
        "HEA": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "HEA", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "HEA", pre_war=True)),
        },
        "HOR": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "HOR", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "HOR", pre_war=True)),
        },
        "OFF": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "OFF", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "OFF", pre_war=True)),
        },
        "OTH": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "OTH", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "OTH", pre_war=True)),
        },
        "TRA": {
            POST_WAR: calc_weibull(*get_weibull_parameters(country_code, "TRA", pre_war=False)),
            PRE_WAR: calc_weibull(*get_weibull_parameters(country_code, "TRA", pre_war=True)),
        },
    }

    return result


def generate_weibull_data(countries: list[str]) -> None:
    """This function calculates the weibull data for these countries."""

    for country in countries:
        path = WEIBULL_STATISTIC_PATH.format(country.lower())
        data = get_weibull_for_country(country)

        write_dict_to_json(data, path)
