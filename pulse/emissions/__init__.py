"""
emissions
---------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module provides an interface to the emission scaling code.
"""

from .emission_calculation import calculate_emissions
from .export import generate_pyam_export

__all__ = ["calculate_emissions", "generate_pyam_export"]
