#!/usr/bin/env python3

"""
Read archaeal GenBank records and store genome and protein information
using Pyspark.

Author: Wouter Zeevat
"""

import sys

sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

# pylint: disable=wrong-import-position
# pylint: disable=import-error
from pyspark.sql import SparkSession
from Bio import SeqIO

def read_genbank(filename):
    """Reads and handles genbank file extracting features"""

    features = []

    for record in SeqIO.parse(filename, "genbank"):

        # Finds all CDS
        cds_tags = []

        for feature in record.features:
            if feature.type == "CDS":
                locus_tag = feature.qualifiers.get("locus_tag", [None])[0]

                if locus_tag:
                    cds_tags.append(locus_tag)

        for feature in record.features:
            
            # Only neccecary
            if feature.type not in ("CDS", "ncRNA", "rRNA", "gene", "protein"):
                continue
            location = str(feature.location)

            if "<" in location or ">" in location: # Remove ambiguous
                continue

            qualifiers = feature.qualifiers
            coding = False

            if feature.type in ("CDS", "protein"):
                coding = True

            elif feature.type == "gene":
                locus_tag = qualifiers.get("locus_tag", [None])[0]

                if locus_tag not in cds_tags:
                    coding = False
                else:
                    continue

            features.append({
                "accession": record.id,
                "species": record.annotations.get("organism", ""),
                "type": feature.type,
                "location": location,
                "length": len(feature),
                "coding": coding
            })

    return features


def question_1(df):
    """How many features does an Archaeal genome have on average? """
    counts = df.groupBy("accession").count()

    # average
    result = counts.agg({"count": "avg"}).first()[0]

    print("\nQuestion 1:")
    print("Average number of features per genome:", result)


def question_2(df):
    """What is the ratio between coding and non-coding features?
 """
    # Count coding and non-coding features
    counts = df.groupBy("coding").count().collect()

    coding = 0
    non_coding = 0

    for row in counts:
        if row["coding"]:
            coding = row["count"]
        else:
            non_coding = row["count"]

    ratio = coding / non_coding if non_coding > 0 else None

    print("\nQuestion 2:")
    print("Coding features:", coding)
    print("Non-coding features:", non_coding)
    print("Coding/non-coding ratio:", ratio)


def question_3(df):
    """
    What is the minimal and maximal number of proteins in a 
    genome for any of the organisms in the file?
    """
    counts = df.filter(df.type == "CDS") \
               .groupBy("accession") \
               .count()

    result = counts.agg(
        {"count": "min"}
    ).first()[0]

    maximum = counts.agg(
        {"count": "max"}
    ).first()[0]

    print("\nQuestion 3:")
    print("Minimum number of proteins:", result)
    print("Maximum number of proteins:", maximum)


def question_4(df):
    """What is the average length of a feature?
 """
    result = df.agg({"length": "avg"}).first()[0]

    print("\nQuestion 4:")
    print("Average feature length:", result)


def question_5(df):
    """
    Remove all non-coding (RNA) features and 
    save the cleaned-up version as a new DataFrame (Spark format).
    """
    cleaned_df = df.filter(df.coding)

    print("\nQuestion 5:")
    print("Removed non-coding features")
    print("Remaining features:", cleaned_df.count())

    return cleaned_df


def question_6(df):
    """
    What is the average length of a feature in this cleaned-up version?
    """
    result = df.agg({"length": "avg"}).first()[0]

    print("\nQuestion 6:")
    print("Average feature length after cleaning:", result)


if __name__ == "__main__":

    file = sys.argv[1]

    features = read_genbank(file)

    spark = SparkSession.builder \
        .appName("assignment4") \
        .master("local[4]") \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()

    data = spark.createDataFrame(features)

    print("Total number of features:", df.count())

    # Questions
    question_1(data)
    question_2(data)
    question_3(data)
    question_4(data)

    cleaned_data = question_5(data)

    question_6(cleaned_data)

    spark.stop()
