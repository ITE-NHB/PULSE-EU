"""
defines.py
----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains the definitions for the export module.
"""

DEMOLITION = "Demolition"
CONSTRUCTION = "Construction"
REFURBISHMENT = "Renovation"

DEMOLISHED = "Demolished"
NEW = "New"
RENOVATED = "Renovated"
STOCK = "Stock"

RESIDENTIAL = "Residential"
COMMERCIAL = "Commercial"
RES_AND_COMM = "Residential and Commercial"

STAGE_TO_VARIABLE: dict[str, str] = {
    "A1-3": "A1-3 Emissions|CO₂|Construction",
    "A4": "A4 Emissions|CO₂|Transportation",
    "A5": "A5 Emissions|CO₂|Construction Installation",
    "B2": "B2 Emissions|CO₂|Maintenance",
    "B4": "B4 Emissions|CO₂|Replacement",
    "B5": "B5 Emissions|CO₂|Refurbishment",
    "B6": "B6 Emissions|CO₂|Energy",
    "C1": "C1 Emissions|CO₂|Demolition",
    "C2": "C2 Emissions|CO₂|Demolition",
    "C3": "C3 Emissions|CO₂|Demolition",
    "C4": "C4 Emissions|CO₂|Demolition",
}

NUM_OUTPUT_DIGITS = 5

SORT_OUTPUT = True
