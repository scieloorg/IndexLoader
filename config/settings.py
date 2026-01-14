import os

INPUT_DIR = os.getenv("INPUT_DIR", "/data")

OS_INDEX = os.getenv("OS_INDEX", "raw_openalex_works")
OS_NODES = os.getenv("OS_NODES", "https://200.136.72.107")
OS_PORT  = os.getenv("OS_PORT", "9200")
OS_USER  = os.getenv("OS_USER", "admin")
OS_PASS  = os.getenv("OS_PASS", "****")

SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", f"sparkle-{OS_INDEX}")
SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/openalex_by_year_part")
# Padrão dos arquivos de entrada (ajuste se precisar de */*.gz)
INPUT_PATTERN = os.environ.get("INPUT_PATTERN", "*/*.gz")
YEARS_ENV = os.environ.get("YEARS", "2020,2021,2022,2023,2024,2025")  # Ex: "2015,2016,2017"
