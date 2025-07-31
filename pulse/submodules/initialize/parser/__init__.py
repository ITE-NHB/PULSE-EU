"""
parser
------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This Module is the parser to parse all the different raw files into more easily managable data files.
"""

from .data_parser import clear_files, parse_data
from .emission_data import parse_emissions
from .information_logging import log_problems
from .scaling import recalculate_scaling_factors, update_hashes

__all__ = [
    "clear_files",
    "parse_data",
    "parse_emissions",
    "log_problems",
    "recalculate_scaling_factors",
    "update_hashes",
]
