import json
import os

from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import settings
from spark.session import get_spark

def parse_years():
    if settings.YEARS_ENV:
        years = []
        for part in settings.YEARS_ENV.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                years.append(int(part))
            except ValueError:
                print(f"Ignorando valor inválido em YEARS: {part!r}")
        years = sorted(set(years))
        print(f"YEARS definido via env → {years}")
        return years

    default_years = list(range(2020, 2025 + 1))
    print(f"YEARS não definido; usando padrão 2020–2025 → {default_years}")
    return default_years

# UDF para remover o campo do JSON (linha JSONL)
@F.udf(returnType=T.StringType())
def drop_abstract_inverted_index(raw: str) -> str:
    if raw is None:
        return None
    try:
        obj = json.loads(raw)
        obj.pop("abstract_inverted_index", None)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        # Se tiver alguma linha ruim, mantém como está (ou você pode retornar None para descartar)
        return raw

def main():
    spark = get_spark()

    # 1) Ler todos os .gz como TEXTO (cada linha = JSON cru)
    path = f"{settings.INPUT_DIR}/{settings.INPUT_PATTERN}"
    print(f"Lendo arquivos de: {path}")
    df = spark.read.text(path)  # coluna 'value'

    # 2) Descobrir o ano
    df = df.withColumn(
        "aux_year",
        F.get_json_object(F.col("value"), "$.publication_year").cast("int")
    )

    df = df.withColumn(
        "aux_year",
        F.when(F.col("aux_year").isNotNull(), F.col("aux_year"))
         .otherwise(
             F.substring(
                 F.get_json_object(F.col("value"), "$.publication_date"),
                 1,
                 4,
             ).cast("int")
         )
    )

    df = df.filter(F.col("aux_year").isNotNull())

    # 3) Definir anos-alvo e filtrar
    selected_years = parse_years()
    df = df.filter(F.col("aux_year").isin(selected_years))

    years = (
        df.select("aux_year")
          .distinct()
          .orderBy("aux_year")
          .rdd.map(lambda r: r["aux_year"])
          .collect()
    )
    print(f"Anos detectados (após filtro): {years}")

    if not years:
        print("Nenhum registro encontrado para os anos selecionados.")
        spark.stop()
        return

    # 3.1) Remover abstract_inverted_index do JSON bruto (ANTES de escrever)
    df = df.withColumn("value", drop_abstract_inverted_index(F.col("value")))

    # 4) Escrita particionada por ano em formato texto + gzip
    (
        df.select("value", "aux_year")  # garante que só essas colunas vão pro write
        .write
        .mode("overwrite")
        .partitionBy("aux_year")     # pastas aux_year=YYYY/
        .option("compression", "gzip")
        .text(settings.OUTPUT_DIR)            # gera part-...txt.gz
    )

    # 5) Renomear pastas aux_year=YYYY → YYYY
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    Path = jvm.org.apache.hadoop.fs.Path

    out_path = Path(settings.OUTPUT_DIR)

    if not fs.exists(out_path):
        raise RuntimeError(f"Diretório de saída não existe: {settings.OUTPUT_DIR}")

    for status in fs.listStatus(out_path):
        name = status.getPath().getName()
        if name.startswith("aux_year="):
            year = name.split("=", 1)[1]

            if int(year) not in selected_years:
                continue

            old_path = status.getPath()
            new_path = Path(os.path.join(settings.OUTPUT_DIR, year))

            if fs.exists(new_path):
                fs.delete(new_path, True)

            print(f"Renomeando pasta: {name} → {year}")
            fs.rename(old_path, new_path)

    # 6) Renomear part-*.txt.gz → part-*.jsonl.gz
    for y in years:
        year_dir = Path(os.path.join(settings.OUTPUT_DIR, str(y)))
        if not fs.exists(year_dir):
            continue

        for status in fs.listStatus(year_dir):
            fname = status.getPath().getName()
            if not fname.startswith("part-"):
                continue

            old_file = status.getPath()
            parent_str = old_file.getParent().toString()

            if fname.endswith(".txt.gz"):
                base = fname[:-len(".txt.gz")]
                new_name = base + ".jsonl.gz"
            elif fname.endswith(".gz"):
                base = fname[:-3]
                new_name = base + ".jsonl.gz"
            else:
                if fname.endswith(".txt"):
                    base = fname[:-4]
                else:
                    base = fname
                new_name = base + ".jsonl"

            new_file = Path(os.path.join(parent_str, new_name))

            print(f"Renomeando arquivo: {fname} → {new_name}")
            fs.rename(old_file, new_file)

    spark.stop()
    print("🏁 Finalizado com sucesso!")

if __name__ == "__main__":
    main()
