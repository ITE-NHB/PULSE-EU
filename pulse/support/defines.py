"""
defines.py
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains definitions for:
      - Typologies
      - Countries
      - Renovations
      - Weibull
"""

from typing import TypeAlias


# Typology stuff

RESIDENTIAL = "Residential"
NON_RESIDENTIAL = "Non-residential"

RESIDENTIAL_TYPOLOGIES: list[str] = sorted(["SFH", "MFH", "ABL"])
NON_RESIDENTIAL_TYPOLOGIES: list[str] = sorted(["EDU", "HEA", "HOR", "OFF", "OTH", "TRA"])

TYPOLOGIES: list[str] = sorted(RESIDENTIAL_TYPOLOGIES + NON_RESIDENTIAL_TYPOLOGIES)

TYPOLOGIES_ORDERED = ["SFH", "MFH", "ABL", "OFF", "EDU", "HEA", "TRA", "HOR", "OTH"]

TYPOLOGY_SHORT_TO_LONG = {
    "SFH": "Single-family house",
    "MFH": "Multi-family house",
    "ABL": "Apartment block",
    "OFF": "Office",
    "EDU": "Education",
    "HEA": "Health",
    "HOR": "Hotel and restaurant",
    "TRA": "Trade",
    "OTH": "Other",
}

# Indicators

BASIC_INDICATORS: list[str] = [
    "ind_GWP_Tot",
    "ind_GWP_Fos",
    "ind_GWP_Bio",
    "ind_GWP_LuLuc",
    "mj_per_m2_building",
]

OPTIONAL_INDICATORS: list[str] = [
    "ind_ODP",
    "ind_AP",
    "ind_EP_Fw",
    "ind_EP_Mar",
    "ind_EP_Ter",
    "ind_PCOP",
    "ind_ADP_MiMe",
    "ind_ADP_Fos",
    "ind_WDP",
    "ind_PM",
    "ind_IRP",
    "ind_ETP_Fw",
    "ind_HTP_c",
    "ind_HTP_nc",
    "ind_SQP",
    "ind_GWP_EN15804+A1",
]

# Country stuff

COUNTRY_TO_COUNTRY_CODE: dict[str, str] = {
    "Austria": "AT",
    "Belgium": "BE",
    "Greece": "EL",
    "Lithuania": "LT",
    "Portugal": "PT",
    "Bulgaria": "BG",
    "Spain": "ES",
    "Luxembourg": "LU",
    "Romania": "RO",
    "Czechia": "CZ",
    "Czech Republic": "CZ",
    "France": "FR",
    "Hungary": "HU",
    "Slovenia": "SI",
    "Denmark": "DK",
    "Croatia": "HR",
    "Malta": "MT",
    "Slovakia": "SK",
    "Germany": "DE",
    "Italy": "IT",
    "Netherlands": "NL",
    "Finland": "FI",
    "Estonia": "EE",
    "Cyprus": "CY",
    "Sweden": "SE",
    "Ireland": "IE",
    "Latvia": "LV",
    "Poland": "PL",
}

COUNTRY_CODE_TO_COUNTRY: dict[str, str] = {code: country for country, code in COUNTRY_TO_COUNTRY_CODE.items()}

COUNTRY_CODE_LIST: list[str] = list(COUNTRY_CODE_TO_COUNTRY.keys())


def get_country_name(code: str) -> str:
    """This function translates a country code to its full name."""
    if len(code) == 2:
        return COUNTRY_CODE_TO_COUNTRY[code.upper()]

    assert code in COUNTRY_TO_COUNTRY_CODE, f"Country code {code} is not valid."
    return code


def get_country_code(country: str) -> str:
    """This function translates a country name to its EU Code."""
    if len(country) == 2:
        assert country in COUNTRY_CODE_LIST, f"Country {country} is not valid."
        return country

    return COUNTRY_TO_COUNTRY_CODE[country.title()]


CONTINENTAL = "CON"
NORDIC = "NOR"
MEDITERRANIAN = "MED"
OCEANIC = "OCE"


REGION_SHORT_TO_LONG = {
    CONTINENTAL: "Continental",
    NORDIC: "Nordic",
    MEDITERRANIAN: "Mediterranean",
    OCEANIC: "Oceanic",
}

COUNTRY_TO_REGION = {
    "AT": CONTINENTAL,
    "BG": CONTINENTAL,
    "CZ": CONTINENTAL,
    "HU": CONTINENTAL,
    "PL": CONTINENTAL,
    "RO": CONTINENTAL,
    "SK": CONTINENTAL,
    "SI": CONTINENTAL,
    "HR": MEDITERRANIAN,
    "CY": MEDITERRANIAN,
    "EL": MEDITERRANIAN,
    "IT": MEDITERRANIAN,
    "MT": MEDITERRANIAN,
    "PT": MEDITERRANIAN,
    "ES": MEDITERRANIAN,
    "DK": NORDIC,
    "EE": NORDIC,
    "FI": NORDIC,
    "LV": NORDIC,
    "LT": NORDIC,
    "SE": NORDIC,
    "BE": OCEANIC,
    "FR": OCEANIC,
    "DE": OCEANIC,
    "IE": OCEANIC,
    "LU": OCEANIC,
    "NL": OCEANIC,
}

# Renovation stuff

PRE_WAR = "Pre-War"
POST_WAR = "Post-War"

WeibullDataType: TypeAlias = dict[str, dict[str, list[float] | dict[str, list[float]]]]


CONSTRUCTION_TYPES = ["0", "1", "2"]
REFURBISHMENT_TYPES = ["0", "1", "2", "3"]
HEATING_TYPES = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]

B_STAGES: list[str] = ["B2", "B4", "B6"]
REF_STAGES: list[str] = ["B5"]
CON_STAGES: list[str] = ["A1-3", "A4", "A5"]
DEM_STAGES: list[str] = ["C1", "C2", "C3", "C4"]

NEW_CON_EPOCH = (2020, 2050)

FIXED_CONSTRUCTION_AREA_MULTIPLIER = 0.8

LIGHT = "Light"
MEDIUM = "Medium"
DEEP = "Deep"


STAGE_TO_CATEGORY: dict[str, str] = {
    "A1-3": "Construction embodied carbon",
    "A4": "Construction embodied carbon",
    "A5": "Construction embodied carbon",
    "B2": "Use phase embodied carbon",
    "B4": "Use phase embodied carbon",
    "B5": "Renovation embodied carbon",
    "B6": "Use phase operational carbon",
    "C1": "Demolition embodied carbon",
    "C2": "Demolition embodied carbon",
    "C3": "Demolition embodied carbon",
    "C4": "Demolition embodied carbon",
}
