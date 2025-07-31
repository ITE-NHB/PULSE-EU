"""
task_list.py
------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file contains the task list object to create, iterate and store tasks.
"""

from enum import Enum
import os
from threading import Thread
from time import sleep
from typing import Generator
from shutil import rmtree

import polars as pl

from pulse.config import TASKS_PATH
from pulse.support.arguments import InitializeArguments, SchedulerArguments, initializeArgs, schedulerArgs
from pulse.support.defines import COUNTRY_CODE_LIST
from pulse.support.file_interaction import hash_file, load_dict_from_json, write_dict_to_json


class MergeException(Exception):
    """Exception raised when merging files fails."""


def merge_pyams(path: str, scenario: str, files: list[str]) -> None:
    """This function merges the pyam files specified in the list and stores it to a file specified by the path."""
    result = pl.concat([pl.read_csv(file) for file in files])

    years = [col for col in result.columns if col not in ("Model", "Scenario", "Region", "Variable", "Unit")]

    emission_total = (
        result.filter(pl.col("Variable").str.contains("Emissions"))
        .group_by("Model", "Variable", "Unit")
        .agg(pl.col(str(year)).sum() for year in years)
        .with_columns(
            pl.lit(scenario).alias("Scenario"),
            pl.lit("EU").alias("Region"),
        )
        .sort("Variable")
        .select(result.columns)
    )

    activity_data = (
        result.filter(~pl.col("Variable").str.contains("rate") & ~pl.col("Variable").str.contains("Emissions"))
        .group_by("Model", "Variable", "Unit")
        .agg(pl.col(str(year)).sum() for year in years)
        .with_columns(
            pl.lit(scenario).alias("Scenario"),
            pl.lit("EU").alias("Region"),
        )
        .sort("Variable")
        .select(result.columns)
    )

    model = activity_data.item(0, 0)

    rates_dict: dict[str, list[object]] = {
        "Model": [model] * 9,
        "Scenario": [scenario] * 9,
        "Region": ["EU"] * 9,
        "Variable": [],
        "Unit": ["%"] * 9,
    } | {str(year): [] for year in years}

    rates_dict["Variable"].append("Energy Service|Residential and Commercial|Construction rate")
    rates_dict["Variable"].append("Energy Service|Residential|Construction rate")
    rates_dict["Variable"].append("Energy Service|Commercial|Construction rate")
    rates_dict["Variable"].append("Energy Service|Residential and Commercial|Demolition rate")
    rates_dict["Variable"].append("Energy Service|Residential|Demolition rate")
    rates_dict["Variable"].append("Energy Service|Commercial|Demolition rate")
    rates_dict["Variable"].append("Energy Service|Residential and Commercial|Renovation rate")
    rates_dict["Variable"].append("Energy Service|Residential|Renovation rate")
    rates_dict["Variable"].append("Energy Service|Commercial|Renovation rate")

    res_stock_filter = pl.col("Variable").str.contains(r"^Energy Service\|Residential\|.*\|Floor space\|Stock$")
    com_stock_filter = pl.col("Variable").str.contains(r"^Energy Service\|Commercial\|.*\|Floor space\|Stock$")
    res_dem_filter = pl.col("Variable").str.contains(r"^Energy Service\|Residential\|.*\|Demolished$")
    com_dem_filter = pl.col("Variable").str.contains(r"^Energy Service\|Commercial\|.*\|Demolished$")
    res_ref_filter = pl.col("Variable").str.contains(r"^Energy Service\|Residential\|.*\|Renovated$")
    com_ref_filter = pl.col("Variable").str.contains(r"^Energy Service\|Commercial\|.*\|Renovated$")
    res_con_filter = pl.col("Variable").str.contains(r"^Energy Service\|Residential\|.*\|New$")
    com_con_filter = pl.col("Variable").str.contains(r"^Energy Service\|Commercial\|.*\|New$")

    for year in years:
        res_stock = activity_data.filter(res_stock_filter).select(str(year)).sum().item()
        com_stock = activity_data.filter(com_stock_filter).select(str(year)).sum().item()
        res_dem = activity_data.filter(res_dem_filter).select(str(year)).sum().item()
        com_dem = activity_data.filter(com_dem_filter).select(str(year)).sum().item()
        res_ref = activity_data.filter(res_ref_filter).select(str(year)).sum().item()
        com_ref = activity_data.filter(com_ref_filter).select(str(year)).sum().item()
        res_con = activity_data.filter(res_con_filter).select(str(year)).sum().item()
        com_con = activity_data.filter(com_con_filter).select(str(year)).sum().item()

        rates_dict[year].append((res_con + com_con) / (res_stock + com_stock) * 100)
        rates_dict[year].append(res_con / res_stock * 100)
        rates_dict[year].append(com_con / com_stock * 100)
        rates_dict[year].append((res_dem + com_dem) / (res_stock + com_stock) * 100)
        rates_dict[year].append(res_dem / res_stock * 100)
        rates_dict[year].append(com_dem / com_stock * 100)
        rates_dict[year].append((res_ref + com_ref) / (res_stock + com_stock) * 100)
        rates_dict[year].append(res_ref / res_stock * 100)
        rates_dict[year].append(com_ref / com_stock * 100)

    rates = pl.DataFrame(rates_dict).sort("Variable")

    result = pl.concat([emission_total, rates, activity_data, result], rechunk=True)

    result = pl.concat([result.filter(pl.col("Region") == "EU"), result.filter(pl.col("Region") != "EU")])

    result.write_csv(path, include_bom=True)


def merge_parquets(path: str, files: list[str]) -> None:
    """This function merges the parquet files specified in the list and stores it to a file specified by the path."""
    pl.concat([pl.scan_parquet(file) for file in files]).sink_parquet(path, compression="lz4")


class GenerationState(Enum):
    """The state of the generation."""

    NOT_GENERATED = "Not generated"
    GENERATION_DONE = "done"
    GENERATION_FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class ScenarioGroup:
    """The ScenarioGroup class is a container for a group of scenarios."""

    scenarios: dict[str, GenerationState]
    merged = False
    count = 0
    completed_count = 0
    successful_count = 0

    def __init__(self) -> None:
        self.scenarios = {}

    def add_scenario(self, scenario: str) -> None:
        """Add a scenario to the scenario group."""
        assert scenario not in self.scenarios, f"Scenario {scenario} already exists in the group."
        self.scenarios[scenario] = GenerationState.NOT_GENERATED
        assert len(self.scenarios) <= len(COUNTRY_CODE_LIST), "Too many scenarios in the group."

        self.count += 1

    def mark_scenario_as_done(self, scenario: str) -> None:
        """Marks a scenario as done."""
        assert self.scenarios[scenario] != GenerationState.GENERATION_DONE, f"Scenario {scenario} is already done."
        self.scenarios[scenario] = GenerationState.GENERATION_DONE

        self.completed_count += 1
        self.successful_count += 1

    def mark_scenario_as_failed(self, scenario: str) -> None:
        """Marks a scenario as failed."""
        assert self.scenarios[scenario] != GenerationState.GENERATION_DONE, f"Scenario {scenario} is already done."
        self.scenarios[scenario] = GenerationState.GENERATION_FAILED

        self.completed_count += 1

    def to_dict(self) -> dict[str, str]:
        """Converts the scenario group to a dictionary."""
        return {scenario: str(state) for scenario, state in self.scenarios.items()} | {"merged": str(self.merged)}

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "ScenarioGroup":
        """Creates a scenario group from a dictionary."""
        sg = ScenarioGroup()

        sg.merged = data.pop("merged") == "True"

        for scenario, state in data.items():
            assert isinstance(scenario, str), "Scenario must be a string."
            assert isinstance(state, str), "State must be a string."

            g_state = GenerationState(state)
            sg.scenarios[scenario] = g_state
            sg.count += 1
            if g_state != GenerationState.NOT_GENERATED:
                sg.completed_count += 1
            if g_state == GenerationState.GENERATION_DONE:
                sg.successful_count += 1

        return sg

    def __len__(self) -> int:
        return len(self.scenarios)

    def completed_scenario_count(self) -> int:
        """Returns the number of completed scenarios."""
        return list(self.scenarios.values()).count(GenerationState.GENERATION_DONE)

    def merge(self, output_folder: str, name: str, delete_files: bool, pyam: bool) -> None:
        """Merges the scenario files."""
        try:
            if self.merged:
                return

            while self.count != self.completed_count:
                sleep(1)

            if self.completed_count != self.successful_count:
                return

            scenario_files = [
                os.path.join(output_folder, "runs", scenario, "emissions.parquet") for scenario in self.scenarios
            ]

            merged_path = os.path.join(output_folder, "merged", f"{name}.parquet")

            merge_parquets(merged_path, scenario_files)

            if pyam:
                scenario_files = [
                    os.path.join(output_folder, "runs", scenario, f"pyam_{scenario}.csv") for scenario in self.scenarios
                ]

                pyam_path = os.path.join(output_folder, "merged", f"pyam_{name}.csv")

                merge_pyams(pyam_path, name, scenario_files)

            if delete_files:
                for folder in [os.path.join(output_folder, "runs", scenario) for scenario in self.scenarios]:
                    rmtree(folder)

            self.merged = True

        except (KeyboardInterrupt, FileNotFoundError):
            pass
        except Exception as e:
            raise MergeException(f"Error merging files: {e}") from e


class TaskList:
    """The TaskList class is a container for a group of tasks."""

    tasks: dict[str, ScenarioGroup]

    # Arguments
    file_data: dict[str, str]
    runs_folder: str
    countries: list[str]
    full_output: bool
    merge_files: bool
    delete_files: bool
    pyam: bool
    start_year: int
    end_year: int
    step_size: int

    def __init__(self, args: SchedulerArguments | InitializeArguments) -> None:
        self.tasks = {}

        if isinstance(args, SchedulerArguments):
            self.file_data = {
                "strategies_file": args.strategies_file,
                "strategies_hash": hash_file(args.strategies_file),
                "capacities_file": args.capacities_file,
                "capacities_hash": hash_file(args.capacities_file),
                "commitments_file": args.commitments_file,
                "commitments_hash": hash_file(args.commitments_file),
            }
        else:
            self.file_data = {
                "strategies_file": "",
                "strategies_hash": "",
                "capacities_file": "",
                "capacities_hash": "",
                "commitments_file": "",
                "commitments_hash": "",
            }

        self.output_folder = args.output_folder
        self.countries = args.countries
        self.full_output = args.full_output
        self.merge_files = args.merge
        self.delete_files = args.delete_files
        self.pyam = args.create_pyam_export

        self.start_year = args.start_year
        self.end_year = args.end_year
        self.step_size = args.step_size

        if self.delete_files and not self.merge_files:
            raise ValueError("Cannot delete files without merging them first.")

    @classmethod
    def load(cls) -> "TaskList":
        """Loads the task list from a the tasks file."""
        tl = TaskList(schedulerArgs)

        data = load_dict_from_json(TASKS_PATH.format(tl.output_folder))

        scenarios_data = data["scenarios"]
        assert isinstance(scenarios_data, dict), "Scenarios must be a dictionary."

        for commitment_name, scenarios_group in scenarios_data.items():
            assert isinstance(commitment_name, str), "Commitment name must be a string."
            assert isinstance(scenarios_group, dict), "Scenarios must be a dictionary."

            tl.tasks[commitment_name] = ScenarioGroup.from_dict(scenarios_group)

        return tl

    @classmethod
    def generation_run(cls) -> "TaskList":
        """Create a task list for the generation run."""
        tl = TaskList(initializeArgs)

        for country in tl.countries:
            tl.add_scenario(country, "")

        return tl

    def add_scenario(self, scenario: str, commitment_name: str | None = None) -> None:
        """Add a scenario to the task list."""
        if commitment_name is None:
            commitment_name = scenario[scenario.find("-") + 1 :]

        self.tasks.setdefault(commitment_name, ScenarioGroup()).add_scenario(scenario)

    def mark_scenario_as_done(self, scenario: str, commitment_name: str | None = None) -> None:
        """Mark a scenario as done."""
        if commitment_name is None:
            idx = scenario.find("-")
            if idx != -1:
                commitment_name = scenario[idx + 1 :]
            else:
                commitment_name = ""

        if commitment_name not in self.tasks:
            raise KeyError(f"{commitment_name} is not in {list(self.tasks.keys())}")

        self.tasks[commitment_name].mark_scenario_as_done(scenario)

    def mark_scenario_as_failed(self, scenario: str, commitment_name: str | None = None) -> None:
        """Mark a scenario as failed."""
        if commitment_name is None:
            idx = scenario.find("-")
            if idx != -1:
                commitment_name = scenario[idx + 1 :]
            else:
                commitment_name = ""

        if commitment_name not in self.tasks:
            raise KeyError(f"{commitment_name} is not in {list(self.tasks.keys())}")

        self.tasks[commitment_name].mark_scenario_as_failed(scenario)

    def store(self) -> None:
        """Store the task list."""
        data = self.file_data | {
            "output_folder": self.output_folder,
            "countries": self.countries,
            "full_output": self.full_output,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "step_size": self.step_size,
            "scenarios": {
                commitment_name: scenario_group.to_dict() for commitment_name, scenario_group in self.tasks.items()
            },
        }

        write_dict_to_json(data, TASKS_PATH.format(self.output_folder))

    def __len__(self) -> int:
        return sum(len(scenario_group) for scenario_group in self.tasks.values())

    def completed_scenario_count(self) -> int:
        """Returns the number of completed scenarios."""
        return sum(scenario_group.completed_scenario_count() for scenario_group in self.tasks.values())

    def get_scenarios(self) -> Generator[str | None, None, None]:
        """Get the next scenario to run."""
        threads: list[Thread] = []
        started_idx = -1

        for comm_name, scenario_group in self.tasks.items():
            for scenario, state in scenario_group.scenarios.items():
                if state != GenerationState.GENERATION_DONE:
                    if threads:
                        if started_idx < len(threads) - 1 and not threads[started_idx].is_alive():
                            started_idx += 1
                            threads[started_idx].start()

                    yield scenario

            if self.merge_files and not scenario_group.merged:
                t = Thread(
                    target=scenario_group.merge,
                    kwargs={
                        "output_folder": self.output_folder,
                        "name": comm_name,
                        "delete_files": self.delete_files,
                        "pyam": self.pyam,
                    },
                    daemon=True,
                )
                threads.append(t)

        if threads and started_idx != -1:
            while threads[started_idx].is_alive():
                yield None

        while started_idx < len(threads) - 1:
            if not threads[started_idx].is_alive():
                started_idx += 1
                threads[started_idx].start()

            while threads[started_idx].is_alive():
                yield None

    def get_failed_scenarios(self) -> Generator[str, None, None]:
        """Get the failed scenarios."""
        for scenario_group in self.tasks.values():
            for scenario, state in scenario_group.scenarios.items():
                if state == GenerationState.GENERATION_FAILED:
                    yield scenario
