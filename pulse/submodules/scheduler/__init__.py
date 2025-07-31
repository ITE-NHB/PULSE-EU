"""
scheduler
---------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    The scheduler allows the parallel execution of scenarios for the Pulse EU model.
"""

from .generate_and_schedule import start_scheduler
from .scheduler import start_generation_scheduler

__all__ = ["start_scheduler", "start_generation_scheduler"]
