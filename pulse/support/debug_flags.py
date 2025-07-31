"""
debug_flags.py
--------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains flags for:
        - Data Assertion:
            - CHECK_DATA_INTEGRITY
            - CHECK_DISTRIBUTE_DATA_INTEGRITY
            - CHECK_FILTERS_GET_DATA

        - Debug printing
            - PRINT_SELECTED_SCENARIO
"""

# Error checking

CHECK_DATA_INTEGRITY: bool = True
"""Enabling/Disabling this controls whether the bulding data is checked after each major modification.\n
Minor Impact
-"""

CHECK_DISTRIBUTE_DATA_INTEGRITY: bool = True
"""Enabling/Disabling this controls whether the distribution functions check the validity of the passed and generated
data.\n
Minor Impact
-"""

CHECK_FILTERS_GET_DATA: bool = True
"""Enabling/Disabling this controls whether the Filters for the emissions check whether any data matches.\n
Minor Impact
-"""


# Debugging

PRINT_SELECTED_SCENARIO: int = 2
"""Controls the level of Scenario information shown:\n
0: No information shown\n
1: Scenario name shown\n
2: Basic Scenario info shown\n
3: Detailed Scenario info shown
"""
