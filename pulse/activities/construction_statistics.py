"""
construction_statistics.py
--------------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This file provides a construction statistics object that holds the construction statistics data.
"""

from dataclasses import dataclass


@dataclass
class ConstructionStatistics:
    """A class for the construction statistics."""

    typology_distribution: dict[str, float]
    construction: dict[str, dict[str, float]]
    energy_performance: dict[str, dict[str, float]]
    heating: dict[str, dict[str, float]]
    sqm_per_person: dict[str, float]
