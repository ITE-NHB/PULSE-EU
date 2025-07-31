"""
initialize.py
-------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains the initialization of files for the model.
"""

from pulse.submodules.scheduler import scheduler
from pulse.support.defines import COUNTRY_CODE_LIST
from pulse.support.ui import time_function

from .parser import clear_files, parse_data, log_problems, update_hashes, parse_emissions, recalculate_scaling_factors


@time_function("Initialization", time_string="time")
def initialize_data(verbose: bool = False) -> None:
    """Initialize the model."""
    print("Running the parser...")
    clear_files()

    parse_data(verbose)
    if verbose:
        print("Parsing emission data...")
    parse_emissions(COUNTRY_CODE_LIST)

    if verbose:
        print("Generating virtual population...")
    scheduler.start_generation_scheduler()

    if verbose:
        print("Recalculating scaling factors...")
    recalculate_scaling_factors(COUNTRY_CODE_LIST)

    if verbose:
        print("Scaling emissions...")
    parse_emissions(COUNTRY_CODE_LIST)

    if verbose:
        print("Updating scaling hashes...")
    update_hashes(COUNTRY_CODE_LIST)

    print("Logging problems... ", end="", flush=True)
    log_problems()
    print("done.")
