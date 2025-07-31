"""
scenario_generator
------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The scenario generation submodule generates the scenarios from a spreadsheet file.
"""

from .scenario_generator import generate_scenarios
from .task_list import TaskList

__all__ = ["generate_scenarios", "TaskList"]
