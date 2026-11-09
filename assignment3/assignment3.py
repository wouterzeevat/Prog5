
#!/usr/bin/env python3

"""
Read archaeal GenBank records and store genome and protein information
in a MariaDB database.

Author: Wouter Zeevat
"""

import configparser
from pathlib import Path

import yaml
import sqlalchemy
from Bio import SeqIO
from sqlalchemy import create_engine, text

if sqlalchemy.__version__.startswith("1.4"):
    from sqlalchemy.engine import URL
elif sqlalchemy.__version__.startswith("2"):
    from sqlalchemy.engine.url import URL
else:
    raise RuntimeError("Requires SQLAlchemy 1.4 or 2.x")


def read_genbank(filename):

    genomes = []

    for record in SeqIO.parse(filename, "genbank"):

        genome = {
            "species": record.annotations.get("organism", ""),
            "accession": record.id,
            "size": len(record.seq),
            "genes": 0,
            "proteins": []
        }

        for feature in record.features:

            qualifiers = feature.qualifiers

            if feature.type == "gene":
                genome["genes"] += 1

            if feature.type == "source":
                for reference in qualifiers.get("db_xref", []):
                    if reference.startswith("taxon:"):
                        genome["taxid"] = reference.split(":")[1]

            if feature.type == "CDS":

                protein_id = qualifiers.get("protein_id", [None])[0]

                if protein_id:
                    go = []

                    # Qual
                    for key in ("go_function", "go_process", "go_component"):
                        go.extend(qualifiers.get(key, []))

                    for reference in qualifiers.get("db_xref", []):
                        if reference.startswith("GO:"):
                            go.append(reference)

                    genome["proteins"].append({
                        "id": protein_id,
                        "product": qualifiers.get("product", [None])[0],
                        "location": str(feature.location),
                        "locus_tag": qualifiers.get("locus_tag", [None])[0],
                        "gene": qualifiers.get("gene", [None])[0],
                        "ec": qualifiers.get("EC_number", []),
                        "go": list(dict.fromkeys(go))
                    })

        genomes.append(genome)

    return genomes


def connect_database():
    """Connects to SQL mariadb database from mycnf file"""


    config = configparser.ConfigParser(interpolation=None)
    config.read(Path.home() / ".my.cnf")

    if "client" not in config:
        raise RuntimeError("Missing [client] section in ~/.my.cnf")

    db = config["client"]

    url = URL.create(
        "mysql+mysqldb",
        username=db["user"],
        password=db["password"],
        host=db.get("host", "mariadb.bin.bioinf.nl"),
        port=db.getint("port", 3306),
        database=db.get("database", db.get("db"))
    )

    engine = create_engine(url)
    print("Connected to database")

    return engine


def create_tables(engine):
    """Creates empty SQL tables to be filled in later"""

    # The tables are separated because one genome can contain many proteins. 
    # The Genome table stores information about each genome, while the Protein table stores information 
    # about the proteins belonging to each genome. 
    # They are connected using the accession number. This way, we don't have to store the same genome information for every protein.

    with engine.begin() as connection:

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS Genome (
                accession VARCHAR(100) PRIMARY KEY,
                species_name VARCHAR(255),
                genome_size BIGINT,
                number_of_genes INT,
                number_of_proteins INT,
                taxid VARCHAR(32)
            )
        """))

        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS Protein (
                protein_id VARCHAR(100),
                accession VARCHAR(100),
                product TEXT,
                location VARCHAR(255),
                locus_tag VARCHAR(100),
                gene VARCHAR(100),
                ec_numbers TEXT,
                go_annotations TEXT,
                PRIMARY KEY (protein_id, accession),
                FOREIGN KEY (accession) REFERENCES Genome(accession)
            )
        """))

    print("Tables created")


def insert_data(engine, genomes):
    """Puts data into the SQL tabvles"""


    with engine.begin() as connection:

        for genome in genomes:

            connection.execute(text("""
                INSERT IGNORE INTO Genome
                VALUES (:accession, :species, :size, :genes, :proteins, :taxid)
            """), {
                "accession": genome["accession"],
                "species": genome["species"],
                "size": genome["size"],
                "genes": genome["genes"],
                "proteins": len(genome["proteins"]),
                "taxid": genome.get("taxid")
            })

            proteins = [
                {
                    "id": p["id"],
                    "accession": genome["accession"],
                    "product": p["product"],
                    "location": p["location"],
                    "locus_tag": p["locus_tag"],
                    "gene": p["gene"],
                    "ec": ", ".join(p["ec"]) or None,
                    "go": ", ".join(p["go"]) or None
                }
                for p in genome["proteins"]
            ]

            if proteins:
                connection.execute(text("""
                    INSERT IGNORE INTO Protein
                    VALUES (:id, :accession, :product, :location,
                            :locus_tag, :gene, :ec, :go)
                """), proteins)

    print("Data inserted")


def main():

    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    genomes = read_genbank(config["genbank_file"])

    print("Read", len(genomes), "GenBank records")

    engine = connect_database()

    try:
        create_tables(engine)
        insert_data(engine, genomes)

        with engine.connect() as connection:
            for table in ("Genome", "Protein"):
                count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {table}")
                ).scalar_one()

                print(table, ":", count, "rows")

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
