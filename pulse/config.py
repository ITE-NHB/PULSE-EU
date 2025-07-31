"""
config.py
---------

Author: Nicolas Bechstedt\\
Supervision: Nicolas Alaux\\
Other contributors: See README.md\\
License: See LICENSE.md

Description:
    All the config data that may be changed. E.g. edit a path in this list to change the file/folder location.
"""

### Raw input files ###

RAW_DATA_PATH = "data/raw/"

ARCHETYPE_STOCK_RAW_DATA = RAW_DATA_PATH + "ArchetypeStockData/{}.parquet"
ARCHETYPE_EMISSION_RAW_DATA = RAW_DATA_PATH + "ArchetypeEmissionData/{}.parquet"

POPULATION_1800_2021_RAW_DATA = RAW_DATA_PATH + "Population/Population_1800_2021.csv"
POPULATION_2019_2050_RAW_DATA = RAW_DATA_PATH + "Population/Population_2019_2050.xlsx"
POPULATION_DISTRIBUTION_RAW_DATA = RAW_DATA_PATH + "Population/population_type_distribution.csv"

FLOOR_AREA_INCREASE_RAW_DATA = RAW_DATA_PATH + "floor_area_increase/floor_area_increase_calibrated.json"

CONSTRUCTION_RATES_NON_RES_RAW_DATA = RAW_DATA_PATH + "construction/reference_construction_rate_non_res.csv"
CONSTRUCTION_EP_RATES_RAW_DATA = RAW_DATA_PATH + "construction/construction_ep_rates.csv"

RENOVATION_DISTRIBUTION_RAW_DATA = RAW_DATA_PATH + "/refurbishment/refurbishment_distribution.json"

WEIBULL_PARAMETERS_RAW_DATA = RAW_DATA_PATH + "demolition/weibull.json"

REFERENCE_B6_2020_PATH = RAW_DATA_PATH + "reference_b6_2020.json"


### Parsed Files ###

PARSED_DATA_PATH = "data/parsed/"

ARCHETYPE_STOCK_DATA_PATH = PARSED_DATA_PATH + "ArchetypeStockData/{}.parquet"
EMISSION_DATA_PATH = PARSED_DATA_PATH + "ArchetypeEmissionData/{}.parquet"

POPULATION_STATISTIC_PATH = PARSED_DATA_PATH + "population/{}.json"
VIRTUAL_POPULATION_PATH = PARSED_DATA_PATH + "v_population/{}.json"

FLOOR_AREA_STATISTIC_PATH = PARSED_DATA_PATH + "floor_area/{}.json"

CONSTRUCTION_RATES_NON_RES_PATH = PARSED_DATA_PATH + "construction/construction_rates_non_res.json"
CONSTRUCTION_EP_RATES_PATH = PARSED_DATA_PATH + "construction/energy_performance_rates.json"

REFURBISHMENT_DISTRIBUTION_PATH = PARSED_DATA_PATH + "refurbishment/refurbishment_distribution.json"
REFURBISHMENT_RATES_DATA = PARSED_DATA_PATH + "refurbishment/refurbishment_rates.json"

WEIBULL_STATISTIC_PATH = PARSED_DATA_PATH + "weibull/{}.json"

SCALING_FACTOR_PATH = PARSED_DATA_PATH + "scaling_b6.json"

PARSER_LOG_PATH = "{}/parser.log"


### Default input files ###

STRATEGIES_PATH = "data/Strategies/Strategies.xlsx"
CAPACITIES_PATH = "data/Capacities/Capacities.xlsx"


### Output Files ###

DEFAULT_OUTPUT_FOLDER = "output/"

PYAM_EXPORT_PATH = "{}/pyam_{}.csv"
EMISSION_EXPORT_PATH = "{}/emissions.parquet"

ACTIVITIES_FOLDER = "{}/activities/"

LOG_PATH = "{}/logs/{}.log"
OUTPUT_LOG_PATH = "{}/output.log"

BUILDING_STOCK_PATH = ACTIVITIES_FOLDER + "building_stock.parquet"
DEMOLITIONS_PATH = ACTIVITIES_FOLDER + "demolitions.parquet"
REFURBISHMENTS_PATH = ACTIVITIES_FOLDER + "refurbishments.parquet"
CONSTRUCTIONS_PATH = ACTIVITIES_FOLDER + "constructions.parquet"

EMISSIONS_FOLDER = "{}/emissions/"

SCENARIORIZED_EMISSIONS_PATH = EMISSIONS_FOLDER + "archetype_emission_data_scenariorized_{}.parquet"

B2B4B6_EMISSIONS_PATH = EMISSIONS_FOLDER + "b2b4b6_emissions.parquet"
DEMOLITION_EMISSIONS_PATH = EMISSIONS_FOLDER + "demolition_emissions.parquet"
REFURBISHMENT_EMISSIONS_PATH = EMISSIONS_FOLDER + "refurbishment_emissions.parquet"
CONSTRUCTION_EMISSIONS_PATH = EMISSIONS_FOLDER + "construction_emissions.parquet"

# Output folder plus this path
MERGED_EMISSIONS_PATH = "merged/"


### Scenario Files ###

TASKS_PATH = "{}/scenario input/tasks.json"
SCENARIO_PATH = "{}/scenario input/{}.pickle"
