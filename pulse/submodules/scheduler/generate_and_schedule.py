"""
generate_and_schedule.py
------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file controls whether the scenarios get regenerated ans starts the scheduler
"""

import os
from pulse.config import TASKS_PATH
from pulse.support.arguments import schedulerArgs

from pulse.submodules.scenario_generator import generate_scenarios
from pulse.support.file_interaction import hash_file, load_dict_from_json
from .scheduler import run_scheduler


def tasks_are_up_to_date() -> bool:
    """This function checks if the scenarios are up to date."""
    args = schedulerArgs

    tasks_path = TASKS_PATH.format(args.output_folder)

    if args.reset or not os.path.exists(tasks_path):
        return False

    tasks_data = load_dict_from_json(tasks_path)

    return all(
        (
            tasks_data["strategies_file"] == args.strategies_file,
            tasks_data["strategies_hash"] == hash_file(args.strategies_file),
            tasks_data["capacities_file"] == args.capacities_file,
            tasks_data["capacities_hash"] == hash_file(args.capacities_file),
            tasks_data["commitments_file"] == args.commitments_file,
            tasks_data["commitments_hash"] == hash_file(args.commitments_file),
            tasks_data["output_folder"] == args.output_folder,
            tasks_data["countries"] == args.countries,
            tasks_data["full_output"] == args.full_output,
            tasks_data["start_year"] == 2020,
            tasks_data["end_year"] == 2050,
            tasks_data["step_size"] == args.step_size,
        )
    )


def start_scheduler() -> None:
    """This function starts the scheduler."""
    args = schedulerArgs

    try:
        os.makedirs(os.path.join(args.output_folder, "merged"), exist_ok=True)

        if not tasks_are_up_to_date() or args.generate_only:
            generate_scenarios()
        else:
            print("Resuming from existing tasks.")

        if not args.generate_only:
            run_scheduler()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user outside of process scheduling.")
