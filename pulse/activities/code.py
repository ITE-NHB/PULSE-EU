"""
code.py
-------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains code classes and methods.
"""

from pulse.support.archetype_data import get_last_building_epoch_start
from pulse.support.defines import RESIDENTIAL, RESIDENTIAL_TYPOLOGIES


def typology_to_full_name(typology: str) -> str:
    """This function gets either "Residential" if the typology is Residential or the full name if not"""
    full_name = ""

    match typology:
        case "SFH":
            full_name = RESIDENTIAL
        case "MFH":
            full_name = RESIDENTIAL
        case "ABL":
            full_name = RESIDENTIAL
        case _:
            full_name = typology

    return full_name


def get_use_from_code(code: str) -> str:
    """This function gets the usage from the code."""
    return typology_to_full_name(get_typology_from_code(code))


def code_is_residential(code: str) -> bool:
    """This function gets whether the code is a residential archetype."""
    return get_typology_from_code(code) in RESIDENTIAL_TYPOLOGIES


def get_country_from_code(code: str) -> str:
    """This function gets the country letters from the code."""
    return code[0:2]


def get_typology_from_code(code: str) -> str:
    """This function gets the typology letters from the code."""
    return code[3:6]


def get_epoch_start_from_code(code: str) -> int:
    """This function gets the epoch start from the code."""
    return int(code[7:11])


def get_epoch_end_from_code(code: str) -> int:
    """This function gets the epoch end from the code."""
    return int(code[12:16])


def code_is_exb(code: str) -> bool:
    """Checks whether the code is of type REF and has refurbishment level 1"""
    return code[17:20] == "EXB"


def code_is_ref(code: str) -> bool:
    """Checks whether the code is of type REF."""
    return code[17:20] == "REF"


def code_is_new(code: str) -> bool:
    """Checks whether the code is of type NEW."""
    return code[17:20] == "NEW"


def get_construction_type_from_code(code: str) -> str:
    """This function gets the construction type from the code."""
    return code[22:23]


def get_refurb_type_from_code(code: str) -> str:
    """This function gets the refurbishment type from the code."""
    return code[25:26]


def get_heating_type_from_code(code: str) -> str:
    """This function gets the heating type from the code."""
    return code[27:29]


def code_is_demolishable(code: str) -> bool:
    """Checks whether the code not of type REF or has refurbishment level 1"""
    return code[17:20] != "REF" or get_refurb_type_from_code(code) == "1"


def code_is_refurbishable(code: str) -> bool:
    """Checks whether the code is of type EXB."""
    return code[17:20] == "EXB" and get_epoch_start_from_code(code) != get_last_building_epoch_start()


def code_is_repurposable(code: str) -> bool:
    """Checks whether the code is of type REF and has refurbishment level 1"""
    return code_is_refurbishable(code) or (code_is_ref(code) and get_refurb_type_from_code(code) == "1")


def get_refurbed_code(code: str, new_ref: str) -> str:
    """This function gets the code with the refurbishment level specified."""
    new_ref = new_ref[-1]

    return f"{code[:16]}-REF-00-0{new_ref}-0{new_ref}"


# Example code:
# AT-EDU-1850-1944-REF-00-01-01
# AT: Country
# -
# EDU: Typology
# -
# 1850: Start of archetype construction epoch
# -
# 1944: End of archetype construction epoch
# -
# REF: Type of the archetype (EXB, REF, or NEW)
# -
# 0: FREE REAL ESTATE
# 0: Construction Type
# -
# 0: FREE REAL ESTATE
# 1: Refurbishment Type
# -
# 01:  Heating type
