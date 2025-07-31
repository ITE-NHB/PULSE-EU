"""
file_interaction.py
-------------------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains functions that read and get data from files.
"""

import hashlib
from io import BytesIO
import json
import os
import pickle
from typing import Any

import polars as pl

from pulse.config import (
    ARCHETYPE_STOCK_DATA_PATH,
    CONSTRUCTION_EP_RATES_PATH,
    CONSTRUCTION_RATES_NON_RES_PATH,
    EMISSION_DATA_PATH,
    EMISSION_EXPORT_PATH,
    POPULATION_STATISTIC_PATH,
    FLOOR_AREA_STATISTIC_PATH,
    PYAM_EXPORT_PATH,
    REFURBISHMENT_DISTRIBUTION_PATH,
    REFURBISHMENT_RATES_DATA,
    SCALING_FACTOR_PATH,
    VIRTUAL_POPULATION_PATH,
    WEIBULL_STATISTIC_PATH,
    BUILDING_STOCK_PATH,
    CONSTRUCTIONS_PATH,
    DEMOLITIONS_PATH,
    REFURBISHMENTS_PATH,
)

from pulse.support.defines import NON_RESIDENTIAL, RESIDENTIAL
from pulse.support.logging import log_info


### General functions ###


def __get_dataframe_from_parquet(path: str) -> pl.DataFrame:
    """This function loads a DataFrame from the specified file."""
    assert os.path.isfile(path), f'"{os.path.abspath(path)}" is not a valid parquet file!'

    return pl.read_parquet(path)


def __get_lazyframe_from_parquet(path: str) -> pl.LazyFrame:
    """This function loads a DataFrame from the specified file."""
    assert os.path.isfile(path), f'"{os.path.abspath(path)}" is not a valid parquet file!'

    return pl.scan_parquet(path)


def __write_dataframe_to_csv(data: pl.DataFrame, path: str) -> None:
    """This function writes a DataFrame to the specified file."""
    folder: str = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)

    if os.path.isfile(path):
        os.remove(path)

    data.write_csv(path, include_bom=True)


def __write_lazyframe_to_parquet(data: pl.LazyFrame, path: str) -> None:
    """This function writes a DataFrame to the specified file."""
    folder: str = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)

    if os.path.isfile(path):
        os.remove(path)

    data.sink_parquet(path, row_group_size=1024 * 1024 * 128)


def __write_dataframe_to_parquet(data: pl.DataFrame, path: str) -> None:
    """This function writes a DataFrame to the specified file."""
    folder: str = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)

    if os.path.isfile(path):
        os.remove(path)

    data.write_parquet(path)


def read_dataframe_from_excel(path: str, sheet: str | None) -> pl.DataFrame:
    """This function reads a DataFrame from the specified excel file and sheet."""
    assert os.path.isfile(path), f'"{os.path.abspath(path)}" is not a valid excel file!'

    if sheet is None:
        return pl.read_excel(path)

    return pl.read_excel(path, sheet_name=sheet)


def load_dict_from_json(path: str) -> dict[str, Any]:
    """This function loads a dictionary from the specified file."""
    assert os.path.isfile(path), f'"{os.path.abspath(path)}" is not a valid json file!'

    with open(path, "r", encoding="UTF-8") as file:
        data: dict[str, Any] = json.load(file)
        assert isinstance(data, dict)

        return data


def load_dict_from_pickle(path: str) -> dict[str, Any]:
    """This function loads a dictionary from the specified file."""
    assert os.path.isfile(path), f'"{os.path.abspath(path)}" is not a valid json file!'

    with open(path, "rb") as file:
        data: dict[str, Any] = pickle.load(file)
        assert isinstance(data, dict)

        return data


def write_dict_to_json(data: dict[Any, Any], path: str) -> None:
    """This function writes a json dict to the specified path"""
    folder: str = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)

    if os.path.isfile(path):
        os.remove(path)

    with open(path, "w", encoding="UTF-8") as file:
        json.dump(data, file, indent=4)


### Storing data ###


def store_virtual_population(data: dict[int, int], country: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(VIRTUAL_POPULATION_PATH.format(country.lower()))
    write_dict_to_json(data, path)

    log_info(f'Stored virtual population data to "{path}"')


def store_building_stock(data: pl.DataFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(BUILDING_STOCK_PATH.format(path))
    __write_dataframe_to_parquet(data, path)

    log_info(f'Stored building stock activity data to "{path}"')


def store_demolitions(data: pl.DataFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(DEMOLITIONS_PATH.format(path))
    __write_dataframe_to_parquet(data, path)

    log_info(f'Stored demolition activity data to "{path}"')


def store_refurbishments(data: pl.DataFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(REFURBISHMENTS_PATH.format(path))
    __write_dataframe_to_parquet(data, path)

    log_info(f'Stored refurbishment activity data to "{path}"')


def store_construction(data: pl.DataFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(CONSTRUCTIONS_PATH.format(path))
    __write_dataframe_to_parquet(data, path)

    log_info(f'Stored construction activity data to "{path}"')


def store_emissions(data: pl.LazyFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(path)
    __write_lazyframe_to_parquet(data, path)

    log_info(f'Stored emission data to "{path}"')


def store_emissions_export(data: pl.LazyFrame, path: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(EMISSION_EXPORT_PATH.format(path))

    __write_lazyframe_to_parquet(data, path)

    log_info(f'Stored emission export to "{path}"')


def store_pyam_export(data: pl.DataFrame, path: str, scenario_name: str) -> None:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(PYAM_EXPORT_PATH.format(path, scenario_name))
    __write_dataframe_to_csv(data, path)

    log_info(f'Stored PyamExport to "{path}"')


### Loading data ###


def load_archetype_dataframe(country: str) -> pl.DataFrame:
    """This function loads the archetype dataframe from the file."""
    path = ARCHETYPE_STOCK_DATA_PATH.format(country.lower())

    assert os.path.isfile(path), f'Unable to open archetype dataframe at "{path}"'

    return __get_dataframe_from_parquet(path)


def load_population_statistics(country: str) -> dict[int, int]:
    """This function loads the population statistics for the specified country."""
    path = POPULATION_STATISTIC_PATH.format(country.lower())

    assert os.path.isfile(path), f'Unable to open population statistics at "{path}"'

    return {int(year): pop for year, pop in load_dict_from_json(path).items()}


def load_virtual_population(country: str) -> dict[int, int]:
    """This function stores the Data supplied into the folder specified."""
    path = os.path.abspath(VIRTUAL_POPULATION_PATH.format(country.lower()))

    assert os.path.isfile(path), f'Unable to open population statistics at "{path}"'

    return {int(year): pop for year, pop in load_dict_from_json(path).items()}


def load_floor_area_statistics(country: str) -> dict[int, float]:
    """This function loads the population statistics for the specified country."""
    path = FLOOR_AREA_STATISTIC_PATH.format(country.lower())

    assert os.path.isfile(path), f'Unable to open floor area statistics at "{path}"'

    return {int(year): pop for year, pop in load_dict_from_json(path).items()}


def load_weibull(country: str) -> dict[str, dict[str, list[float]]]:
    """This function gets the weibull data."""
    path = WEIBULL_STATISTIC_PATH.format(country.lower())

    assert os.path.isfile(path), f'Unable to open weibull data at "{path}"'

    return load_dict_from_json(path)


def load_scaling_factors(country: str) -> dict[str, float]:
    """This function gets the scaling factors for a given country. (res, non-res)"""
    default = {RESIDENTIAL: 1.0, NON_RESIDENTIAL: 1.0}
    if not os.path.exists(SCALING_FACTOR_PATH):
        return default

    scaling_factors = load_dict_from_json(SCALING_FACTOR_PATH)

    factors_dict: dict[str, float] = scaling_factors.get(country.upper(), default)
    assert isinstance(factors_dict, dict)

    factors_dict.pop("emission_data hash", -1.0)
    factors_dict.pop("archetype_data hash", -1.0)

    return factors_dict


def load_energy_performance_data(country: str) -> dict[str, float]:
    """This function gets the energy performance data for a given country."""
    ep_data: dict[str, dict[str, float]] = load_dict_from_json(CONSTRUCTION_EP_RATES_PATH)

    assert country in ep_data

    country_ep_data = ep_data[country]

    assert isinstance(country_ep_data, dict)

    return country_ep_data


def load_archetype_emissions(country: str) -> pl.LazyFrame:
    """This function loads the archetype emission data from the data files."""
    path = EMISSION_DATA_PATH.format(country.lower())

    assert os.path.isfile(path), f'Unable to open archetype emission data at "{path}"'

    return __get_lazyframe_from_parquet(path)


def load_minimum_construction_rate(country: str, residential: bool) -> float:
    """This function loads the archetype emission data from the data files."""
    if residential:
        raise NotImplementedError("Expected Residential construction rate isn't implemented yet.")

    path = CONSTRUCTION_RATES_NON_RES_PATH

    assert os.path.isfile(path), f'Unable to open construction rates data at "{path}"'

    data = load_dict_from_json(path)[country]
    assert isinstance(data, (float, int))

    return data


def load_std_refurbishment_rates(country: str, residential: bool) -> dict[str, float]:
    """This function loads the standard refurbishment rates from the data files."""
    path = REFURBISHMENT_RATES_DATA

    if residential:
        data: dict[str, float] = load_dict_from_json(path)[country][RESIDENTIAL]
    else:
        data = load_dict_from_json(path)[country][NON_RESIDENTIAL]

    assert isinstance(data, dict)
    return data


def load_refurbishment_distribution() -> dict[str, dict[str, float]]:
    """This function loads the refurbishment distribution data from the data files."""
    path = os.path.abspath(REFURBISHMENT_DISTRIBUTION_PATH)

    assert os.path.isfile(path), f'Unable to open refurbishment distribution data at "{path}"'

    return load_dict_from_json(path)


### Other ###


def hash_file(file: str | BytesIO) -> str:
    """Returns the SHA-256 hash of the file or None if the file doesn't exist."""
    sha256_hash = hashlib.sha256(usedforsecurity=False)

    if isinstance(file, BytesIO):
        offset = file.tell()

        file.seek(0)
        for byte_block in iter(lambda: file.read(4096), b""):
            sha256_hash.update(byte_block)
        file.seek(offset)
    else:
        if not os.path.exists(file):
            return ""

        # Open the file in binary mode and read it in chunks
        with open(file, "rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)

    # Return the hexadecimal digest of the hash
    return sha256_hash.hexdigest()
