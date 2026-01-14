from config import settings

def write_to_opensearch(df, index):
    (
        df.write
        .format("org.opensearch.spark.sql")
        .option("opensearch.nodes", settings.OS_NODES)
        .option("opensearch.port", settings.OS_PORT)
        .option("opensearch.net.http.auth.user", settings.OS_USER)
        .option("opensearch.net.http.auth.pass", settings.OS_PASS)
        .option("opensearch.nodes.wan.only", "true")
        .option("opensearch.resource", index)
        .mode("append")
        .save()
    )
