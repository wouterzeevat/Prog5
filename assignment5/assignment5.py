#data/datasets/dbNSFP/snpEff_old/data/

#!/usr/bin/env python3

"""
Read dbNSFP data, analyse classifier predictions and save the
top five classifiers in a MariaDB database using Spark.

Author: Wouter Zeevat
"""

import sys
import argparse
import configparser
import os

sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

# pylint: disable=wrong-import-position
# pylint: disable=import-error
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, explode, split, row_number, concat, lit


def get_database_settings():
    """Gets the database settings from the config file."""

    # Get database
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.my.cnf"))

    settings = config["client"]

    return {
        "host": settings.get("host", "mariadb.bin.bioinf.nl"),
        "port": settings.get("port", "3306"),
        "database": settings["database"],
        "user": settings["user"],
        "password": settings["password"]
    }


def count_classifier_predictions(df):
    """Counts the predictions for each classifier."""

    # List of classifiers
    classifiers = [ # MADE WITH AI USING https://www.dbnsfp.org
        "SIFT", "SIFT4G", "PROVEAN", "Polyphen2_HDIV",
        "Polyphen2_HVAR", "MutationTaster", "MutationAssessor",
        "fathmm-XF_coding", "CADD", "VEST4", "DANN", "MetaSVM",
        "MetaLR", "MetaRNN", "Eigen", "Eigen-PC", "M-CAP",
        "REVEL", "MutPred", "MVP", "gMVP", "MPC", "PrimateAI",
        "DEOGEN2", "Aloft", "BayesDel", "ClinPred", "LIST-S2",
        "VARITY", "ESM1b", "AlphaMissense", "PHACTboost",
        "MutFormer", "MutScore", "MisFit", "popEVE", "GPN-MSA"
    ]

    # Count
    results = []
    for classifier in classifiers:
        columns = [
            column for column in df.columns
            if (column == classifier or column.startswith(classifier + "_"))
            and (
                column.endswith("_score")
                or column.endswith("_pred")
                or column.endswith("_rankscore")
            )
        ]

        if not columns:
            continue

        exists = F.lit(False) # Placeholder for null
        for column in columns:
            exists = exists | F.col(f"`{column}`").isNotNull()

        count = df.filter(exists).count()

        results.append((classifier, count))

    return results

def save_to_sql(df, table):
    """
    Saves a dataframe to table in sql
    """
    settings = get_database_settings()
    (
        df.write
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "jdbc:mariadb://mariadb.bin.bioinf.nl/Whzeevat")
        .option("dbtable", table)
        .option("user", settings["user"])
        .option("password", settings["password"])
        .mode("overwrite")
        .save()
    )

def create_foreign_keys(spark):
    """Creates primary and foreign keys in the SQL tables."""

    connection = (
        spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            "jdbc:mariadb://mariadb.bin.bioinf.nl/Whzeevat",
            get_database_settings()["user"],
            get_database_settings()["password"]
        )
    )

    statement = connection.createStatement()

    statement.execute("ALTER TABLE Variant ENGINE=InnoDB")
    statement.execute("ALTER TABLE Protein ENGINE=InnoDB")
    statement.execute("ALTER TABLE VariantProtein ENGINE=InnoDB")

    statement.execute("""
        ALTER TABLE Variant
        MODIFY variant_id VARCHAR(100) NOT NULL,
        ADD PRIMARY KEY (variant_id)
    """)

    statement.execute("""
        ALTER TABLE Protein
        ADD PRIMARY KEY (protein_id)
    """)

    statement.execute("""
        ALTER TABLE VariantProtein
        MODIFY variant_id VARCHAR(100) NOT NULL,
        MODIFY protein_id INT NOT NULL,
        ADD PRIMARY KEY (variant_id, protein_id)
    """)

    statement.execute("""
        ALTER TABLE VariantProtein
        ADD FOREIGN KEY (variant_id) REFERENCES Variant(variant_id)
    """)

    statement.execute("""
        ALTER TABLE VariantProtein
        ADD FOREIGN KEY (protein_id) REFERENCES Protein(protein_id)
    """)

    statement.close()
    connection.close()

def drop_sql_tables(spark):
    """Drops the existing tables, cant overwrite due to foreign keys."""

    settings = get_database_settings()

    properties = spark._jvm.java.util.Properties()
    properties.setProperty("user", settings["user"])
    properties.setProperty("password", settings["password"])

    driver = spark._jvm.org.mariadb.jdbc.Driver()
    connection = driver.connect(
        "jdbc:mariadb://mariadb.bin.bioinf.nl/Whzeevat",
        properties
    )

    statement = connection.createStatement()

    statement.execute("DROP TABLE IF EXISTS VariantProtein")
    statement.execute("DROP TABLE IF EXISTS Protein")
    statement.execute("DROP TABLE IF EXISTS Variant")
    statement.execute("DROP TABLE IF EXISTS dbnsfp_top5_raw")

    statement.close()
    connection.close()


def create_sql_tables(df):
    """Creates and saves the normalized SQL tables."""

    # These tables are chosen because variants and proteins are stored separately to avoid duplicate data. 
    # The Variant table stores each unique genetic variant, while the Protein table stores each unique protein. 
    # The VariantProtein table connects them because one variant can be associated with multiple proteins and one protein can be associated 
    # with multiple variants.

    df = df.withColumnRenamed("#chr", "chr") \
        .withColumnRenamed("pos(1-based)", "pos")

    # Variant primary table
    variant_df = df.select("chr", "pos", "ref", "alt").distinct()
    variant_df = variant_df.withColumn(
        "variant_id",
        concat(col("chr"), lit(":"), col("pos"), lit(":"), col("ref"), lit(":"), col("alt"))
    )

    save_to_sql(df, "dbnsfp_top5_raw")
    save_to_sql(variant_df, "Variant")

    # Protein table
    protein_df = df.select(
        explode(split(col("Ensembl_proteinid"), ";")).alias("Ensembl_proteinid")
    ).distinct()

    protein_df = protein_df.filter(
        (col("Ensembl_proteinid") != ".") &
        (col("Ensembl_proteinid") != "") &
        col("Ensembl_proteinid").isNotNull()
    )

    protein_df = protein_df.withColumn(
        "protein_id",
        row_number().over(Window.orderBy("Ensembl_proteinid"))
    )

    save_to_sql(protein_df, "Protein")

    # VariantProtein table
    variant_protein_df = df.select(
        "chr",
        "pos",
        "ref",
        "alt",
        explode(split(col("Ensembl_proteinid"), ";")).alias("Ensembl_proteinid")
    ).distinct()

    variant_protein_df = variant_protein_df.filter(
        (col("Ensembl_proteinid") != ".") &
        (col("Ensembl_proteinid") != "") &
        col("Ensembl_proteinid").isNotNull()
    )

    # Link
    variant_protein_df = variant_protein_df.withColumn(
        "variant_id",
        concat(col("chr"), lit(":"), col("pos"), lit(":"), col("ref"), lit(":"), col("alt"))
    )

    # Link
    variant_protein_df = variant_protein_df.join(
        protein_df,
        on="Ensembl_proteinid"
    )

    variant_protein_df = variant_protein_df.select(
        "variant_id",
        "protein_id"
    ).distinct()

    save_to_sql(variant_protein_df, "VariantProtein")


def save_results_to_markdown(count_results, top_five, pos_top, prot_top):
    """Saves the results to a Markdown file."""

    with open("assignment5.md", "w") as file:

        file.write("# Assignment 5: Persistence\n\n")

        file.write("## Question 1: Predictions per classifier\n\n")
        for classifier, count in count_results:
            file.write(f"- {classifier}: {count} predictions\n")

        file.write("\n## Question 2: Top five classifiers\n\n")
        for classifier, count in top_five:
            file.write(f"- {classifier}: {count} predictions\n")

        file.write("\n## Question 3: Unique genomic position\n\n")
        file.write("The chromosome and position were combined into one identifier (`chr:pos`).\n")

        file.write("\n## Question 4: Positions with most predictions\n\n")
        for row in pos_top:
            file.write(f"- {row['global_position']}: {row['count']} predictions\n")

        file.write("\n## Question 5: Proteins with most predictions\n\n")
        for row in prot_top:
            file.write(f"- {row['Ensembl_proteinid']}: {row['count']} predictions\n")


def main():
    """Main"""
    parser = argparse.ArgumentParser()

    parser.add_argument("filename", help="Input dbNSFP file")
    parser.add_argument(
        "--jar",
        default="mariadb-java-client-3.5.0.jar"
    )

    args = parser.parse_args()

    # Spark
    spark = (
        SparkSession.builder
        .appName("Assignment5")
        .master("local[*]")
        .config("spark.jars", "/homes/whzeevat/Prog5/assignment5/mariadb-java-client-3.5.0.jar")
        .getOrCreate()
    )

    # Read file
    df = (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("nullValue", ".") #SQL NOTXT
        .option("inferSchema", True)
        .csv(args.filename)
    )


    # Question 1
    count_results = count_classifier_predictions(df)
    print(count_results)
    print(len(count_results))

    # Question 2
    count_results = sorted(count_results, key=lambda x: x[1], reverse=True)
    top_five = count_results[:5]
    print(f"top5: {top_five}")

    top_five_names = [classifier for classifier, count in top_five]

    df = df.select(
        *[
            column for column in df.columns
            if any(column.startswith(c + "_") for c in top_five_names)
            or column in ["#chr", "pos(1-based)", "ref", "alt",
                        "Ensembl_proteinid"]
        ]
    )

    # Question 3
    df = df.withColumn(
        "global_position",
        F.concat_ws(":", F.col("#chr"), F.col("pos(1-based)"))
    )

    # Question 4
    pos_top = df.groupBy("global_position").count().orderBy(F.desc("count")).take(5)

    # Question 5
    prot_top = df.groupBy("Ensembl_proteinid").count().orderBy(F.desc("count")).take(5)

    # SQL PART
    drop_sql_tables(spark) # First remove because overwriate didnt work
    create_sql_tables(df)
    create_foreign_keys(spark)
    # SAve markdown
    save_results_to_markdown(count_results, top_five, pos_top, prot_top)

    spark.stop()

if __name__ == "__main__":
    main()
