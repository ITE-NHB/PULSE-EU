"""
pulse_eu_model.py
-----------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains the model object that is exported by the package.
"""

import os
import sys

from traceback import print_exc

# from pulse.emissions import export
from pulse.activities import BuildingStock, Scenario
from pulse.config import OUTPUT_LOG_PATH

from pulse.support.arguments import runArgs
from pulse.support.defines import COUNTRY_CODE_LIST
from pulse.support.logging import close_log_file, open_log_file

from pulse.emissions import calculate_emissions, generate_pyam_export


class PulseEUModel:
    """
    This Object is a wrapper for the building stock model.\n
    To execute run these functions in this order:\n
        1 PulseEUModel() (initializing the data)\n
        2 run() (calculating activity and emission data)\n
    """

    scenario: Scenario
    """The Scenario data."""
    building_stock: BuildingStock
    """The building stock object."""

    def __init__(self) -> None:
        args = runArgs

        scenario_path = os.path.join(args.output_folder, "runs", args.scenario_name)

        os.makedirs(scenario_path, exist_ok=True)
        open_log_file(scenario_path)

        if args.scheduled:
            self.redirect_output(scenario_path)

        self.scenario = Scenario(args.scenario_name, folder=args.output_folder)

        self.building_stock = BuildingStock(self.scenario)

        self.scenario.print()

    def redirect_output(self, output_path: str) -> None:
        """This function redirects the output to a file."""
        # Open the file in write mode and redirect stdout and stderr to the file

        output_file = open(  # pylint: disable=consider-using-with
            OUTPUT_LOG_PATH.format(output_path), encoding="UTF-8", mode="w"
        )

        sys.stdout = output_file
        sys.stderr = output_file

    def run(self, emissions: bool = True) -> None:
        """This function runs the model."""
        args = runArgs
        scenario_folder = os.path.join(runArgs.output_folder, "runs", args.scenario_name)

        is_generation_run = args.scenario_name in COUNTRY_CODE_LIST

        try:
            self.building_stock.run_prediction(is_generation_run)

            if is_generation_run:
                self.building_stock.store_virtual_population()

            if args.store_activity_files:
                self.building_stock.store_data(scenario_folder)

            if emissions:
                calculate_emissions(
                    scenario_folder,
                    self.scenario,
                    self.building_stock,
                    is_generation_run,
                )
            else:
                generate_pyam_export(scenario_folder, self.scenario, self.building_stock)

        except BaseException as exc:
            close_log_file()

            if args.scheduled:
                print_exc()
                print(exc, flush=True)
                sys.exit(-1)
            raise exc from exc
