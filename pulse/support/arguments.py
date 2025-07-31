"""
arguments.py
------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The arguments object to use in the main entry file and for the model itself.
"""

from enum import Enum
import os
import dataclasses
import argparse
import sys

from pulse.config import CAPACITIES_PATH, DEFAULT_OUTPUT_FOLDER, STRATEGIES_PATH
from pulse.support.defines import COUNTRY_CODE_LIST
from pulse.support.ui import parse_string_to_list


__all__ = [
    "ExecutionMode",
    "executionMode",
    "RunArguments",
    "SchedulerArguments",
    "InitializeArguments",
    "runArgs",
    "schedulerArgs",
    "initializeArgs",
]


class ArgumentException(Exception):
    """Simple Class for argument exceptions"""


class ExecutionMode(Enum):
    """The execution mode of the program."""

    RUN = "run"
    INITIALIZE = "initialize"
    SCHEDULER = "scheduler"

    def __str__(self) -> str:
        return self.value


@dataclasses.dataclass
class RunArguments:
    """This holds the run arguments"""

    scheduled: bool
    """Whether the process was scheduled."""

    scenario_name: str
    """The scenario to run."""

    output_folder: str
    """The output storage folder."""

    full_output: bool
    """Run the model and generate the full output."""

    create_pyam_export: bool
    """Whether the pyam export should be generated."""

    store_activity_files: bool
    """Whether the activity files should be stored."""

    store_preprocessing_files: bool
    """Whether the preprocessing files should be stored."""

    store_activity_emission_files: bool
    """Whether the activity emission files should be stored."""

    def __init__(self, args: argparse.Namespace | None = None) -> None:
        if args is None:
            return

        self.scheduled = args.scheduled
        assert isinstance(self.scheduled, bool), "Scheduled must be a boolean."

        self.scenario_name = args.scenario
        assert isinstance(self.scenario_name, str), "The scenario must be a string."

        self.output_folder = os.path.abspath(args.folder)
        assert isinstance(self.output_folder, str), "The folder must be a string."

        self.full_output = args.full_output
        assert isinstance(self.full_output, bool), "The full output flag must be a boolean."

        self.create_pyam_export = args.create_pyam_export
        assert isinstance(self.create_pyam_export, bool), "The create_pyam_export flag must be a boolean."

        self.store_activity_files = args.store_activity_files
        assert isinstance(self.store_activity_files, bool), "The store_activity_files flag must be a boolean."

        self.store_preprocessing_files = args.store_preprocessing_files
        assert isinstance(self.store_preprocessing_files, bool), "The store_preprocessing_files flag must be a boolean."

        self.store_activity_emission_files = args.store_activity_emission_files
        assert isinstance(
            self.store_activity_emission_files, bool
        ), "The store_activity_emission_files flag must be a boolean."


@dataclasses.dataclass
class SchedulerArguments:
    """This holds the scheduler arguments"""

    scheduled: bool
    """Whether the process was scheduled."""

    output_folder: str
    """The output storage folder."""

    full_output: bool
    """Run the model and generate the full output."""

    commitments_file: str
    """The path to the commitment file to use."""

    strategies_file: str
    """The path to the strategies file to use."""

    capacities_file: str
    """The path to the capacities file to use."""

    countries: list[str]
    """The countries to generate."""

    reset: bool
    """Whether to reset the scenario."""

    merge: bool
    """Whether to merge the scenarios."""

    delete_files: bool
    """Whether to delete the files."""

    create_pyam_export: bool
    """Whether the pyam export should be generated."""

    generate_only: bool
    """Whether to generate the scenario files only."""

    thread_count: int
    """The number of threads to use."""

    start_year: int
    """The start year for the scenarios."""

    end_year: int
    """The end year for the scenarios."""

    step_size: int
    """The step size to use for emission calculations."""

    store_activity_files: bool
    """Whether the activity files should be stored."""

    store_preprocessing_files: bool
    """Whether the preprocessing files should be stored."""

    store_activity_emission_files: bool
    """Whether the activity emission files should be stored."""

    def __init__(self, args: argparse.Namespace | None = None) -> None:
        if args is None:
            return

        self.scheduled = args.scheduled
        assert isinstance(self.scheduled, bool), "Scheduled must be a boolean."

        self.output_folder = os.path.abspath(args.folder)
        assert isinstance(self.output_folder, str), "The folder must be a string."

        self.full_output = args.full_output
        assert isinstance(self.full_output, bool), "The full output flag must be a boolean."

        self.commitments_file = os.path.abspath(args.commitments_file)
        if not os.path.exists(self.commitments_file):
            raise ArgumentException(f"The commitments file {self.commitments_file} does not exist.")

        self.strategies_file = os.path.abspath(args.strategies_file)
        if not os.path.exists(self.strategies_file):
            raise ArgumentException(f"The strategies file {self.strategies_file} does not exist.")

        self.capacities_file = os.path.abspath(args.capacities_file)
        if not os.path.exists(self.capacities_file):
            raise ArgumentException(f"The capacities file {self.capacities_file} does not exist.")

        countries = parse_string_to_list(args.countries)
        if countries is None:
            raise ArgumentException(f"The supplied country list is invalid: {args.countries}")
        self.countries = countries

        for country in self.countries:
            assert isinstance(country, str), "The countries must be a list of strings."
            assert country in COUNTRY_CODE_LIST, f"Country code {country} is not valid."

        self.reset = args.reset
        assert isinstance(self.reset, bool), "The reset flag bust be a boolean."

        self.merge = args.merge
        assert isinstance(self.merge, bool), "The merge flag must be a boolean."

        self.delete_files = args.delete_files
        assert isinstance(self.delete_files, bool), "The delete_files flag must be a boolean."

        self.create_pyam_export = args.create_pyam_export
        assert isinstance(self.create_pyam_export, bool), "The create_pyam_export flag must be a boolean."

        self.generate_only = args.generate_only
        assert isinstance(self.generate_only, bool), "The generate_only flag must be a boolean."

        self.thread_count = args.threads
        assert isinstance(self.thread_count, int), "The thread count must be an integer."
        if self.thread_count < 1:
            raise ArgumentException(f"The thread count must be at least 1. Got {self.thread_count}.")

        self.start_year = 2020
        self.end_year = 2050

        self.step_size = args.emissions_step_size
        assert isinstance(self.step_size, int), "The step size must be an integer."
        if not 1 <= self.step_size <= 10:
            raise ArgumentException(f"The step size must be 1 <= step_size <= 10. Got {self.step_size}.")

        self.store_activity_files = args.store_activity_files
        assert isinstance(self.store_activity_files, bool), "The store_activity_files flag must be a boolean."

        self.store_preprocessing_files = args.store_preprocessing_files
        assert isinstance(self.store_preprocessing_files, bool), "The store_preprocessing_files flag must be a boolean."

        self.store_activity_emission_files = args.store_activity_emission_files
        assert isinstance(
            self.store_activity_emission_files, bool
        ), "The store_activity_emission_files flag must be a boolean."


@dataclasses.dataclass
class InitializeArguments:
    """This holds the initialize arguments"""

    verbose: bool

    output_folder: str

    countries: list[str]

    full_output: bool = False
    merge: bool = False
    delete_files: bool = False
    create_pyam_export: bool = False

    store_activity_files = False
    store_preprocessing_files = False
    store_activity_emission_files: bool = False

    start_year: int = 2020
    end_year: int = 2050
    step_size: int = 5

    thread_count: int = 5
    """The number of threads to use."""

    def __init__(self, args: argparse.Namespace | None = None) -> None:
        if args is None:
            return

        self.verbose = args.verbose
        assert isinstance(self.verbose, bool), "The verbose flag must be a boolean."

        self.output_folder = args.folder
        assert isinstance(self.output_folder, str), "The folder must be a string."
        self.output_folder = os.path.abspath(self.output_folder)
        self.output_folder = os.path.join(self.output_folder, "init")

        self.countries = COUNTRY_CODE_LIST


executionMode: ExecutionMode

runArgs: RunArguments
schedulerArgs: SchedulerArguments
initializeArgs: InitializeArguments


def parse_args() -> None:
    """This function parses the arguments passed"""

    def add_initialize_model_subparser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
        """Adds the initialize model subparser to the parser."""
        subparser = subparsers.add_parser(str(ExecutionMode.INITIALIZE), help="Initializes the model files and data.")

        subparser.add_argument("--verbose", "-v", action="store_true", help="Whether to output debug infos.")

        subparser.add_argument(
            "--folder", type=str, default=DEFAULT_OUTPUT_FOLDER, help="The folder to write the scenario output into."
        )

    def add_run_scenario_subparser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
        """Adds the run subparser to the parser."""
        subparser = subparsers.add_parser(str(ExecutionMode.RUN), help="Runs the specified scenario.")

        subparser.add_argument("scenario", type=str, help="The name of the scenario to run.")
        subparser.add_argument(
            "--folder", type=str, default=DEFAULT_OUTPUT_FOLDER, help="The folder to write the scenario output into."
        )
        subparser.add_argument(
            "-f", "--full-output", action="store_true", help="When set all available indicators will be calculated."
        )
        subparser.add_argument("--create-pyam-export", action="store_true", help="Generate the pyam export.")

        subparser.add_argument(
            "--store-activity-files", action="store_true", help="When set the activity files will be stored."
        )
        subparser.add_argument(
            "--store-preprocessing-files", action="store_true", help="When set the preprocessing files will be stored."
        )
        subparser.add_argument(
            "--store-activity-emission-files",
            action="store_true",
            help="When set the activity emission files will be stored.",
        )

    def add_run_scheduler_subparser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
        """Adds the scheduler subparser to the parser."""
        subparser = subparsers.add_parser(
            str(ExecutionMode.SCHEDULER), help="Runs the scheduler with all the generated scenarios."
        )

        subparser.add_argument("commitments_file", type=str, help="The path to the commitment file to use.")
        subparser.add_argument(
            "--strategies-file", type=str, default=STRATEGIES_PATH, help="The path to the strategies file to use."
        )
        subparser.add_argument(
            "--capacities-file", type=str, default=CAPACITIES_PATH, help="The path to the capacities file to use."
        )
        subparser.add_argument(
            "--folder", type=str, default=DEFAULT_OUTPUT_FOLDER, help="The folder to write the scenario output into."
        )
        subparser.add_argument(
            "--countries", type=str, default=str(COUNTRY_CODE_LIST), help="Which countries to run the scheduler for."
        )
        subparser.add_argument(
            "-f", "--full-output", action="store_true", help="When set all available indicators will be calculated."
        )
        subparser.add_argument("-r", "--reset", action="store_true", help="Force recalculation of all scenarios.")
        subparser.add_argument("--create-pyam-export", action="store_true", help="Generate the pyam export.")
        subparser.add_argument("--merge", action="store_true", help="When set the files will be merged.")
        subparser.add_argument(
            "-d", "--delete-files", action="store_true", help="When set only the merged files will be kept."
        )
        subparser.add_argument(
            "--generate-only",
            action="store_true",
            help="When set the scenario files will be generated but not run.",
        )
        subparser.add_argument(
            "--threads",
            type=int,
            default=5,
            help="The number of threads to use for the scheduler.",
        )
        subparser.add_argument(
            "--emissions-step-size",
            type=int,
            default=5,
            help=(
                "The step size in years to use for emissions. "
                + "Lower values lead to higher output fidelity. "
                + "Must be between 1 and 10."
            ),
        )

        subparser.add_argument(
            "--store-activity-files", action="store_true", help="When set the activity files will be stored."
        )
        subparser.add_argument(
            "--store-preprocessing-files", action="store_true", help="When set the preprocessing files will be stored."
        )
        subparser.add_argument(
            "--store-activity-emission-files",
            action="store_true",
            help="When set the activity emission files will be stored.",
        )

    parser = argparse.ArgumentParser(
        description=(
            """
            This model allows for the Prospective Upscaling of Life cycle Scenarios and Environmental impacts of
            European buildings (PULSE-EU). Using building archetypes from the SLiCE model, it can project the
            environmental impacts of the building stock of a country from the EU 27 based on different parameters and
            scenarios.
            """
        )
    )

    parser.add_argument("-s", "--scheduled", action="store_true", help="Writes the output into an output log file.")

    subparsers = parser.add_subparsers(
        title="Command", dest="command", required=True, description="The command decides which action the model takes."
    )

    add_initialize_model_subparser(subparsers)
    add_run_scenario_subparser(subparsers)
    add_run_scheduler_subparser(subparsers)

    if len(sys.argv) == 1:
        parser.print_help(sys.stdout)
        sys.exit(1)

    args = parser.parse_args()

    global executionMode
    executionMode = ExecutionMode(args.command)

    global initializeArgs
    global runArgs
    global schedulerArgs
    try:
        match (executionMode):
            case ExecutionMode.RUN:
                runArgs = RunArguments(args)
                schedulerArgs, initializeArgs = SchedulerArguments(), InitializeArguments()
            case ExecutionMode.SCHEDULER:
                schedulerArgs = SchedulerArguments(args)
                runArgs, initializeArgs = RunArguments(), InitializeArguments()
            case ExecutionMode.INITIALIZE:
                initializeArgs = InitializeArguments(args)
                runArgs, schedulerArgs = RunArguments(), SchedulerArguments()
    except AssertionError as e:
        raise ArgumentException(f"Argument error: {e}") from e


parse_args()
