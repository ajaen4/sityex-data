from f_api_extractor.processes.geodb import run_geodb_workload
from f_api_extractor.processes.countries_cities import run_count_cities_workload
from f_api_extractor.processes.imf import run_imf_workload
from f_api_extractor.processes.open_meteo import run_open_meteo_workload
from f_api_extractor import config as cfg


def main():
    if cfg.RUN_GEODB:
        run_geodb_workload()
    if cfg.RUN_COUNT_CITIES:
        run_count_cities_workload()
    if cfg.RUN_IMF:
        run_imf_workload()
    if cfg.RUN_OPEN_METEO:
        run_open_meteo_workload()
