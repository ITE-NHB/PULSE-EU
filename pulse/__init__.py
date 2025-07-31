"""
pulse
-----

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This model allows for the Prospective Upscaling of Life cycle Scenarios and Environmental impacts of European\
    buildings (PULSE-EU). Using building archetypes from the SLiCE model, it can project the environmental impacts of\
    the building stock of a country from the EU 27 based on different parameters and scenarios.
"""

from .pulse_eu_model import PulseEUModel
from .submodules.scenario_generator import generate_scenarios
from .submodules.scheduler import start_scheduler
from .submodules.initialize.initialize import initialize_data

__all__ = [
    "PulseEUModel",
    "generate_scenarios",
    "start_scheduler",
    "initialize_data",
]
