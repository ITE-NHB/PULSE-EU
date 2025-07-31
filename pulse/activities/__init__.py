"""
activities
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module provides an interface to the activity prediction model.
"""

from .building_stock import BuildingStock
from .scenario import Scenario

__all__ = ["BuildingStock", "Scenario"]
