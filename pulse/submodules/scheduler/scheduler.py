"""
scheduler.py
------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module is responsible for starting multiple instances of the BuildingStockModel for parallel computation.
"""

import sys
import os
import platform

from time import sleep, time
from typing import Sequence

import subprocess
import psutil

from pulse.support.arguments import InitializeArguments, SchedulerArguments, initializeArgs, schedulerArgs
from pulse.support.ui import format_time

from pulse.submodules.scenario_generator import TaskList

PULSE_MAIN_FILE = "pulse_eu.py"


class SchedulerState:
    """This class stores Processes and their states."""

    tasks: TaskList
    """The list of all tasks to run"""
    max_tasks: int
    """The maximum number of running tasks"""
    running_tasks: list[None | subprocess.Popen[bytes]]
    """The currently running tasks"""
    num_total_tasks: int
    """The total number of tasks that need to be run."""
    num_successful_tasks: int
    """The total number of tasks that successfully finished running."""
    num_failed_tasks: int
    """The total number of tasks that failed to finish running."""

    def __init__(self, tasks: TaskList, args: SchedulerArguments | InitializeArguments) -> None:
        folder = args.output_folder
        full_output = args.full_output
        create_pyam_export = args.create_pyam_export

        store_activity_files = args.store_activity_files
        store_preprocessing_files = args.store_preprocessing_files
        store_activity_emission_files = args.store_activity_emission_files

        available_cores = os.cpu_count()
        available_cores = available_cores - 1 if available_cores else args.thread_count
        self.max_tasks = min(args.thread_count, available_cores)

        self.command = [sys.executable, PULSE_MAIN_FILE, "--scheduled", "run", "--folder", folder]

        if full_output:
            self.command.append("--full-output")

        if create_pyam_export:
            self.command.append("--create-pyam-export")

        if store_activity_files:
            self.command.append("--store-activity-files")

        if store_preprocessing_files:
            self.command.append("--store-preprocessing-files")

        if store_activity_emission_files:
            self.command.append("--store-activity-emission-files")

        # print(f"Full command: {self.command}")

        self.tasks = tasks
        self.running_tasks = [None] * self.max_tasks

        self.num_total_tasks = len(self.tasks) - self.tasks.completed_scenario_count()
        self.num_successful_tasks = 0
        self.num_failed_tasks = 0

        print(f"Initialized Tasklist with {self.max_tasks} maximum Threads.")

    def start_process(self, argument: str) -> None:
        """This method starts a process and adds it to the running_tasks list."""
        assert os.path.isfile(PULSE_MAIN_FILE), f"Unable to find {os.path.abspath(PULSE_MAIN_FILE)}"
        assert self.running_tasks.count(None) > 0, "Tried to start a process without a free spot."

        core = self.running_tasks.index(None)

        # Command to run (for example: 'python main.py Scenario-007')
        command = self.command + [argument]

        with open(os.devnull, "w", encoding="UTF-8") as devnull:
            # Start the process
            new_process = subprocess.Popen(  # pylint: disable=consider-using-with
                command,
                stdout=devnull,
                stderr=devnull,
            )

        if platform.system() != "Darwin":
            psutil.Process(new_process.pid).cpu_affinity([core])

        self.running_tasks[core] = new_process

    def check_completed_tasks(self) -> None:
        """This method checks for completed tasks."""
        for core, process in enumerate(self.running_tasks):
            if process is None or process.poll() is None:
                continue

            assert isinstance(process.args, Sequence)

            argument = process.args[-1]
            assert isinstance(argument, str)

            if process.returncode == 0:
                self.tasks.mark_scenario_as_done(argument)
                self.num_successful_tasks += 1
            else:
                self.tasks.mark_scenario_as_failed(argument)
                self.num_failed_tasks += 1

            self.running_tasks[core] = None

    def print_final_info(self) -> None:
        """This method print a summary of the schedules runs."""
        print(f"Successfully finished {self.num_successful_tasks}/{self.num_total_tasks} Scenarios.")

        if self.num_failed_tasks != 0:
            max_arguments_to_print = 5

            print(f"{self.num_failed_tasks} Scenarios failed:")

            for i, scenario in enumerate(self.tasks.get_failed_scenarios()):
                if i >= max_arguments_to_print or i == self.num_failed_tasks:
                    break

                print(f'    -"{scenario}"')

            if self.num_failed_tasks > max_arguments_to_print:
                print("    ...")

    @property
    def is_free(self) -> bool:
        """The number of currently running tasks."""
        return None in self.running_tasks

    @property
    def is_empty(self) -> bool:
        """The number of currently running tasks."""
        return all(task is None for task in self.running_tasks)

    @property
    def finished_tasks(self) -> int:
        """The total number of tasks that finished running."""
        return self.num_failed_tasks + self.num_successful_tasks

    def completion_string(self, no_backspace: bool = False) -> str:
        """The total number of tasks that finished running."""
        red = "\033[31m"
        green = "\033[32m"
        default = "\033[0m"

        string = ""

        total_len_chars = len(str(self.num_total_tasks))

        if not no_backspace:
            string += "\b" * (total_len_chars * 2 + 1)

        string += green if self.num_failed_tasks == 0 else red

        string += f"{str(self.finished_tasks).rjust(total_len_chars)}{default}/{self.num_total_tasks}"

        return string


def schedule_processes(scheduler_state: SchedulerState) -> None:
    """This function schedules instances of the building stock model."""
    print(f"Completed processes: {scheduler_state.completion_string(no_backspace=True)}", flush=True, end="")

    for scenario in scheduler_state.tasks.get_scenarios():
        scheduler_state.check_completed_tasks()

        while not scheduler_state.is_free and scenario is not None:
            sleep(0.1)
            scheduler_state.check_completed_tasks()

        print(f"{scheduler_state.completion_string()}", flush=True, end="")

        if scenario is None:
            sleep(0.1)
            continue

        scheduler_state.start_process(scenario)

    while not scheduler_state.is_empty:
        sleep(0.2)
        scheduler_state.check_completed_tasks()
        print(f"{scheduler_state.completion_string()}", flush=True, end="")

    print(f"{scheduler_state.completion_string()}", flush=True)


def run_scheduler() -> None:
    """This function starts the scheduler."""

    if platform.system() != "Darwin":
        cpu_count = os.cpu_count() or 1
        psutil.Process().cpu_affinity([cpu_count - 1])

    scheduler_state = SchedulerState(TaskList.load(), schedulerArgs)

    start = time()

    try:
        schedule_processes(scheduler_state)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")

    end = time()

    print(f"Calculations took {format_time(seconds=end - start)}")

    scheduler_state.tasks.store()
    scheduler_state.print_final_info()

    if scheduler_state.num_total_tasks != scheduler_state.num_successful_tasks:
        sys.exit(-1)


def start_generation_scheduler() -> None:
    """This function starts the scheduler."""

    scheduler_state = SchedulerState(TaskList.generation_run(), initializeArgs)

    start = time()

    try:
        schedule_processes(scheduler_state)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")

    end = time()

    print(f"Calculations took {format_time(seconds=end - start)}")

    scheduler_state.print_final_info()

    if scheduler_state.num_total_tasks != scheduler_state.num_successful_tasks:
        sys.exit(-1)
