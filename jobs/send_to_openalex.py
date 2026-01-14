from spark.session import get_spark
from config import settings
from utils.opensearch import write_to_opensearch

def main():
    spark = get_spark()

    df = spark.read.json(settings.INPUT_DIR)

    write_to_opensearch(df, settings.OS_INDEX)

    spark.stop()

if __name__ == "__main__":
    main()
