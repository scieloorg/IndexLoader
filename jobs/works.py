from pyspark.sql import functions as F

from config import settings
from spark.session import get_spark
from utils.opensearch import write_to_opensearch

def main():
    spark = get_spark()
    spark.conf.set("spark.sql.caseSensitive", "true")
    df = spark.read.option("multiLine", "false").json(f"{settings.INPUT_DIR}/*.gz")

    df = df.drop("abstract_inverted_index")

    print(f"\nTotal de registros lidos: {df.count()}")
    print(f"Colunas detectadas: {df.columns}\n")

    for c in ["publication_date", "updated_date", "created_date", "updated"]:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))

    if "publication_date" in df.columns:
        df = df.withColumn("pub_year", F.year("publication_date"))

    # Mostra alguns registros que serão enviados
    print("Amostra dos dados que serão enviados para o OpenSearch:")
    df.select("id", "display_name", "pub_year").show(5, truncate=False)

    write_to_opensearch(df, settings.OS_INDEX)

    print("Envio concluído com sucesso!\n")

    spark.stop()