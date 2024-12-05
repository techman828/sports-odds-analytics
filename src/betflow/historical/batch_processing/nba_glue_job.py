import sys

# from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
<<<<<<< HEAD
from pyspark.sql.functions import explode
from betflow.historical.config import ProcessingConfig
=======
>>>>>>> main
from pyspark.sql import SparkSession


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "date",
        "source_path",
        "database_name",
        "table_name",
        "warehouse_path",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)

<<<<<<< HEAD
# spark = glueContext.spark_session
=======
>>>>>>> main
spark = (
    SparkSession.builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    .config("spark.sql.catalog.glue_catalog.warehouse", args["warehouse_path"])
    .getOrCreate()
)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Create database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{args['database_name']}")

# spark.sql(
#     f"DROP TABLE IF EXISTS glue_catalog.{ProcessingConfig.GLUE_DB['db_name']}.{ProcessingConfig.GLUE_DB['nba_games_table']}"
# )

# Create table with proper schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{args['database_name']}.{args['table_name']} (
        game_id STRING,
        start_time TIMESTAMP,
        partition_year INT,
        partition_month INT,
        partition_day INT,
        status_state STRING,
        status_detail STRING,
        status_description STRING,
        period INT,
        clock STRING,
        home_team STRUCT<
            id: STRING,
            name: STRING,
            abbreviation: STRING,
            score: STRING,
            field_goals: STRING,
            three_pointers: STRING,
            free_throws: STRING,
            rebounds: STRING,
            assists: STRING
        >,
        away_team STRUCT<
            id: STRING,
            name: STRING,
            abbreviation: STRING,
            score: STRING,
            field_goals: STRING,
            three_pointers: STRING,
            free_throws: STRING,
            rebounds: STRING,
            assists: STRING
        >,
        venue STRUCT<
            name: STRING,
            city: STRING,
            state: STRING
        >,
        broadcasts ARRAY<STRING>,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (partition_year, partition_month, partition_day)
""")


raw_path = f"{args['source_path']}{args['date']}/games.json"
try:
    df = spark.read.json(raw_path)
    if df.count() == 0:
        print(f"No games found for date: {args['date']}")
        job.commit()
        sys.exit(0)
    df.createOrReplaceTempView("raw_games")

    # Modify the transformation SQL to include partition columns
    processed_df = spark.sql("""
        SELECT 
            game_id,
            CAST(start_time as timestamp) as start_time,
            CAST(YEAR(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_year,
            CAST(MONTH(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_month,
            CAST(DAY(TO_TIMESTAMP(start_time, "yyyy-MM-dd'T'HH:mm'Z'")) as INT) as partition_day,
            status_state,
            status_detail,
            status_description,
            CAST(period as int) as period,
            clock,
            STRUCT(
                home_team_id as id,
                home_team_name as name,
                home_team_abbreviation as abbreviation,
                home_team_score as score,
                home_team_field_goals as field_goals,
                home_team_three_pointers as three_pointers,
                home_team_free_throws as free_throws,
                home_team_rebounds as rebounds,
                home_team_assists as assists
            ) as home_team,
            STRUCT(
                away_team_id as id,
                away_team_name as name,
                away_team_abbreviation as abbreviation,
                away_team_score as score,
                away_team_field_goals as field_goals,
                away_team_three_pointers as three_pointers,
                away_team_free_throws as free_throws,
                away_team_rebounds as rebounds,
                away_team_assists as assists
            ) as away_team,
            STRUCT(
                venue_name as name,
                venue_city as city,
                venue_state as state
            ) as venue,
            broadcasts,
            CAST(TIMESTAMP_SECONDS(CAST(timestamp as LONG)) as TIMESTAMP) as ingestion_timestamp
        FROM raw_games
        WHERE status_state = 'post'
    """)

    # # Add before writing
    # print("Partition Values:")
    # processed_df.select("partition_year", "partition_month", "partition_day").show()
    # processed_df.printSchema()
    if processed_df.count() > 0:
        (
            processed_df.writeTo(
                f"glue_catalog.{args['database_name']}.{args['table_name']}"
            )
            .tableProperty("format-version", "2")
            .option("check-nullability", "false")
            .option("merge-schema", "true")
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("partition_year", "partition_month", "partition_day")
            .append()
        )
    job.commit()

except Exception as e:
    if "no such file or directory" in str(e).lower():
        print(f"No data file found for date: {args['date']}")
        job.commit()
        sys.exit(0)
