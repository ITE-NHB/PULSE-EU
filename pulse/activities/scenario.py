"""
scenario.py
-----------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    This module contains the scenario class.
"""

import os

from pulse.config import SCENARIO_PATH

from pulse.support.defines import COUNTRY_CODE_LIST, get_country_code, get_country_name
from pulse.support.debug_flags import PRINT_SELECTED_SCENARIO
from pulse.support.file_interaction import load_dict_from_pickle
from pulse.support.archetype_data import get_buildingstock_end_year, get_buildingstock_start_year, init_helpers

from .construction_statistics import ConstructionStatistics
from .scenario_parameters import (
    IncreaseNewParameter,
    RepurposeDemoParameter,
    PopulationParameter,
    FloorAreaParameter,
    ShareOfConEpHeaParameter,
    ShareOfNewParameter,
    ShareOfRefParameter,
    UseOfEmptyParameter,
)


class Scenario:
    """A class for a Scenario."""

    model: str
    """The calculation model to use for this Scenario."""
    name: str
    """The scenario name."""
    country: str
    """The region to apply this scenario to."""
    country_code: str
    """The country code of the scenario."""
    start: int
    """The first year of the scenario. Should be the year just after the Building Stock ends."""
    end: int
    """The last year of the scenario."""
    step_size: int
    """The step size of the emission calculations."""
    data_start: int
    """The first year of building stock data."""
    data_end: int
    """The last year of building stock data."""
    population: PopulationParameter
    floor_area: FloorAreaParameter
    use_of_empty: UseOfEmptyParameter
    increase_new: IncreaseNewParameter
    share_of_new: ShareOfNewParameter
    share_con_ep_hea: ShareOfConEpHeaParameter
    residential_ref: ShareOfRefParameter
    non_residential_ref: ShareOfRefParameter
    repurpose_demo: RepurposeDemoParameter

    def __init__(self, scenario_name: str, folder: str) -> None:
        """Creates a new Scenario"""
        if scenario_name not in COUNTRY_CODE_LIST:
            scenario_path = os.path.abspath(SCENARIO_PATH.format(folder, scenario_name))
            assert os.path.isfile(scenario_path), f'Unable to find "{scenario_path}"'

            # Read the scenario data from the file
            data = load_dict_from_pickle(scenario_path)

            model = data.pop("Model")
            name = data.pop("Name")
            country = data.pop("Country")
            start = data.pop("Start")
            end = data.pop("End")
            step_size = data.pop("Step size")

            assert isinstance(model, str)
            assert isinstance(name, str)
            assert isinstance(country, str)
            assert isinstance(start, int)
            assert isinstance(end, int)
            assert isinstance(step_size, int)

            # Initialize basic Scenario data
            self.model = model
            self.name = name
            self.country = country
            self.start = start
            self.end = end
            self.step_size = step_size
        else:
            self.model = "TU Graz"
            self.name = scenario_name
            self.country = get_country_name(scenario_name)
            self.start = 2020
            self.end = 2050
            self.step_size = 5

            data = {}

        assert (
            self.name == scenario_name
        ), f"The scenario name and file name do not match! File name: {scenario_name}, Scenario name: {self.name}"

        for key in list(data.keys()):
            if data[key]["implementation"] != "stock":
                # log_info(f'Skipping parsing for Parameter "{key}"')
                data.pop(key)

        self.country_code = get_country_code(self.country)

        init_helpers(self.country_code, self.start - 1)

        years = (self.start, self.end)

        self.data_start = get_buildingstock_start_year()
        self.data_end = get_buildingstock_end_year()

        assert self.data_end == self.start - 1, "The building stock data end does not match up with the scenario start!"

        # Parse Scenario infos

        self.population = PopulationParameter(data, years)
        self.floor_area = FloorAreaParameter(data, years)
        self.use_of_empty = UseOfEmptyParameter(data, years)
        self.increase_new = IncreaseNewParameter(data, years)
        self.share_of_new = ShareOfNewParameter(data, years)
        self.share_con_ep_hea = ShareOfConEpHeaParameter(data, years)
        self.residential_ref = ShareOfRefParameter(data, years, self.country_code, residential=True)
        self.non_residential_ref = ShareOfRefParameter(data, years, self.country_code, residential=False)
        self.repurpose_demo = RepurposeDemoParameter(data, years)

        assert len(data) == 0, f"There is still scenario data present when it should be empty! data: {data}"

    def __str__(self) -> str:
        string = f'[{self.model}] Scenario "{self.name}" in {self.country} ({self.start} - {self.end})\n\n'

        string += str(self.population) + "\n"
        string += str(self.floor_area) + "\n"
        string += str(self.use_of_empty) + "\n"
        string += str(self.increase_new) + "\n"
        string += str(self.share_of_new) + "\n"
        string += str(self.share_con_ep_hea) + "\n"
        string += str(self.residential_ref) + "\n"
        string += str(self.non_residential_ref) + "\n"
        string += str(self.repurpose_demo) + "\n"

        longest_line_length = max(len(line.strip()) for line in string.splitlines())

        string = "-" * longest_line_length + "\n" + string + "-" * longest_line_length

        return string

    def print(self, level: int = PRINT_SELECTED_SCENARIO) -> None:
        """Prints the scenario data to an optionally specified level of detail."""
        if level == 0:
            return
        if level == 1:
            print(f'Scenario "{self.name}"')
            return
        if level == 2:
            print(f'[{self.model}] Scenario "{self.name}" in {self.country} ({self.start} - {self.end})\n')
            return
        if level == 3:
            print(str(self))
            return

    def get_scenario_construction_statistics(self, year: int) -> ConstructionStatistics:
        """Returns the ConstructionStatistics object for a given year."""

        con_data, ep_data, hea_data = self.share_con_ep_hea[year]

        share_of_new = {t: v for t, v in self.share_of_new[year].items() if v > 0}
        increase_new = {t: v for t, v in self.increase_new[year].items() if v > 0}

        return ConstructionStatistics(
            share_of_new,
            con_data,
            ep_data,
            hea_data,
            increase_new,
        )
