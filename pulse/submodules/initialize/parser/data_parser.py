"""
data_parser.py
--------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module manages the parsing of the raw files
"""

import os
import shutil

from pulse.config import (
    ARCHETYPE_STOCK_DATA_PATH,
    EMISSION_DATA_PATH,
    PARSED_DATA_PATH,
)

from pulse.submodules.initialize.parser.refurbishment_distribution import parse_refurbishment_distribution
from pulse.support.defines import COUNTRY_CODE_LIST

from .population import parse_population
from .floor_area import generate_floor_areas
from .weibull import generate_weibull_data
from .energy_performance import parse_energy_performance
from .refurbishment_rates import parse_refurbishment_rates
from .archetype_data import parse_archetype_data
from .construction_rates import parse_construction_rates


def clear_files() -> None:
    """This function clears all saved files."""
    folders = [
        os.path.dirname(PARSED_DATA_PATH),
        os.path.dirname(ARCHETYPE_STOCK_DATA_PATH),
        os.path.dirname(EMISSION_DATA_PATH),
    ]

    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        shutil.rmtree(folder)

    for folder in folders:
        os.makedirs(folder, exist_ok=True)


def parse_data(verbose: bool = False) -> None:
    """The main entry point for running the parsers"""

    if verbose:
        print("Parsing population data...")
    parse_population(COUNTRY_CODE_LIST)

    if verbose:
        print("Generating weibull data...")
    generate_weibull_data(COUNTRY_CODE_LIST)

    if verbose:
        print("Parsing energy performance data...")
    parse_energy_performance(COUNTRY_CODE_LIST)

    if verbose:
        print("Parsing refurbishment rates data...")
    parse_refurbishment_rates(COUNTRY_CODE_LIST)

    if verbose:
        print("Parsing refurbishment distribution data...")
    parse_refurbishment_distribution()

    if verbose:
        print("Parsing construction rates data...")
    parse_construction_rates(COUNTRY_CODE_LIST)

    if verbose:
        print("Parsing archetype data...")
    parse_archetype_data(COUNTRY_CODE_LIST)

    if verbose:
        print("Generating floor area...")
    generate_floor_areas(COUNTRY_CODE_LIST)
