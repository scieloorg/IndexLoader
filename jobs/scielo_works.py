import os
from spark.session import get_spark
from config import settings
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, MapType


spark = get_spark()

def normalize_date_expr(c_col, dtype):
    if isinstance(dtype, StructType):
        field_names = {f.name for f in dtype.fields}
        if "$date" in field_names:
            s = c_col.getField("$date").cast("string")
        elif "date" in field_names:
            s = c_col.getField("date").cast("string")
        else:
            s = c_col.cast("string")
    elif isinstance(dtype, MapType):
        s = F.coalesce(
            F.element_at(c_col, "$date").cast("string"),
            F.element_at(c_col, "date").cast("string"),
            c_col.cast("string")
        )
    else:
        raw = F.trim(c_col.cast("string"))
        s = F.coalesce(
            F.get_json_object(raw, "$.$date"),
            F.get_json_object(raw, "$.date"),
            raw
        )

    s = F.when((s == "") | (s == "null") | (s == "None"), F.lit(None)).otherwise(s)

    return F.coalesce(
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(s, "yyyy-MM-dd")
    )

def main():

    df = (spark.read
          .option("multiLine", "false")
          .option("dropFieldIfAllNull", "true")
          .json(f"{settings.INPUT_DIR}/*.json"))

    # 1) Identidade e Remoção de _id original
    if "id" not in df.columns and "_id" in df.columns:
        dtype_id = df.schema["_id"].dataType
        df = df.withColumn("id", F.col("_id.$oid") if isinstance(dtype_id, StructType) else F.col("_id").cast("string"))
    
    df = df.drop("_id")

    # 2) Datas principais
    date_cols = ["publication_date", "updated_date", "created_date", "updated", "created_at"]
    for c in date_cols:
        if c in df.columns:
            df = df.withColumn(c, normalize_date_expr(F.col(c), df.schema[c].dataType))

    # 3) Caso especial: processing_date
    if "processing_date" in df.columns:
        ts_expr = normalize_date_expr(F.col("processing_date"), df.schema["processing_date"].dataType)
        df = df.withColumn("processing_date", 
            F.when(ts_expr.isNull(), F.lit(None))
            .otherwise(F.struct(F.date_format(ts_expr, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("date")))
        )

    # 4) Tratamento de conflito: article e subcampos vXXX
    # Se 'article' for um tipo complexo, convertemos para JSON para evitar falha de mapping
    if "article" in df.columns:
        art_type = df.schema["article"].dataType
        if isinstance(art_type, (StructType, MapType)):
            df = df.withColumn("article", F.to_json(F.col("article")))
        else:
            # Se já for string, apenas garante o cast
            df = df.withColumn("article", F.col("article").cast("string"))    

    # 5 ) Escrita
    writer = (
        df.write.format("org.opensearch.spark.sql")
        .option("opensearch.nodes", settings.OS_NODES)
        .option("opensearch.port", settings.OS_PORT)
        .option("opensearch.nodes.wan.only", "true")
        .option("opensearch.net.ssl", "true")
        .option("opensearch.net.ssl.verification_mode", "none")
        .option("opensearch.net.ssl.cert.allow.self.signed", "true")
        .option("opensearch.mapping.id", "id")
        .option("opensearch.batch.size.entries", "10000")
        .option("opensearch.batch.size.bytes", "10mb")
        .option("opensearch.http.timeout", "5m")
        .mode("append")
    )

    if settings.OS_USER and settings.OS_PASS:
        writer = writer.option("opensearch.net.http.auth.user", settings.OS_USER)\
                       .option("opensearch.net.http.auth.pass", settings.OS_PASS)

    writer.save(settings.OS_INDEX)
    spark.stop()

if __name__ == "__main__":
    main()