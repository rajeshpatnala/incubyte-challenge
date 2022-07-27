"""
This module creates CSV files used for vaccination statistics.
Below are list of files processed -

    1. Australia - AUS.xlsx
    2. India     - IND.csv
    3. USA       - USA.csv

Folder information
input_files - Input raw files are placed here.
output_files - output files
                    |- vaccination_contribution
                    |- vaccination_count
                    |- vaccination_percentage

Dependency Input files:
1. input_files directory. 

Dependency Python files:
1. constansts.py

This Module download the required dependency jar files for following package
com.crealytics:spark-excel_2.12:0.13.4. This jar is used to read microsoft excel files.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, udf, to_date, when, col,length
from pyspark.sql.types import *
from constants import INPUT_DATASETS_DIR, COUNTRY_POPULATION


def _get_input_files_info(input_path: str) -> list:
    """
    Returns a list of files path under the
    directory input_path. 
    Args:
        input_path (str): Input Directory Path. 

    Returns:
        list: List of input files.
    """
    _input_data_files = []
    for file_name in os.listdir(input_path):
        _input_data_files.append(os.path.join(input_path, file_name))
    return _input_data_files


def _init_spark() -> SparkSession:
    """
    Initializes and Returns the SparkSession.
    Returns:
        SparkSession: Local SparkSession
    """
    spark = SparkSession \
                .builder \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.4") \
                .config("spark.sql.legacy.timeParserPolicy", "Legacy") \
                .config("spark.sql.shuffle.partitions", "4") \
                .master("local[2]") \
                .appName("spark-session") \
                .getOrCreate()
    return spark


def read_input_file(spark: SparkSession, input_file: str) -> DataFrame:
    """
    Reads input file and returns DataFrame.
    Supported File Formats:
    1. csv
    2. xlsx
    Args:
        spark (SparkSession): Spark Session
        input_file (str): Input File

    Returns:
        DataFrame: Input File DataFrame
    """
    file_ext = os.path.splitext(input_file)[1]
    data_df = spark.createDataFrame([], StructType([]))
    if file_ext == ".csv":
        data_df = spark.read \
                       .option("inferSchema", "False") \
                       .option("header", "true") \
                       .csv(input_file)
    elif file_ext == ".xlsx":
        data_df = spark.read \
                       .format("com.crealytics.spark.excel") \
                       .option("header", "true") \
                       .load(input_file)
    return data_df


def _change_to_date(date: str):
    date = date[:2] + '-' + date[2:4] + '-' + date[4:]
    return date

_change_to_date_udf = udf(_change_to_date)

def transform_date_column(raw_df: DataFrame) -> DataFrame:
    """
    Converts the date string to date.
    Supported Formats:
        yyyy-mm-dd
        mm/dd/yyyy
        mmddyyyy

    Args:
        raw_df (DataFrame): Input Raw DataFrame

    Returns:
        DataFrame: DataFrame with converted date columns.
    """
    cleaned_df = raw_df.withColumn("vaccination_date", \
                                     when(to_date(col("vaccination_date"), "y-M-d").isNotNull(), to_date(col("vaccination_date"), "y-M-d")) \
                                    .when(to_date(col("vaccination_date"), "M/d/y").isNotNull(), to_date(col("vaccination_date"), "M/d/y")) \
                                    .when(col("vaccination_date").rlike(r"\d{8}"), to_date(_change_to_date_udf(col("vaccination_date")), "M-d-y")) \
                                    .otherwise(col("vaccination_date"))
                                  )
    return cleaned_df


def transform_data(raw_df: DataFrame, final_df: DataFrame,
                   country: str) -> DataFrame:
    """
    Returns Transformed DataFrame contains two
    columns country, vaccination_type.
    Args:
        raw_df (DataFrame): Raw DataFrame
        final_df (DataFrame): Final Transformed DataFrame
        country (str): Country String

    Returns:
        DataFrame: Transformed DataFrame
    """
    # Extracting the columns from raw_df.
    raw_df_headers = raw_df.columns

    # Renaming below column names.
    # VaccinationType -> vaccination_type
    # Vaccine Type -> vaccination_type
    for old_header, new_header in zip(["VaccinationType", "Vaccine Type", "Date of Vaccination", "VaccinationDate"],
                                      ["vaccination_type", "vaccination_type", "vaccination_date", "vaccination_date"]):
        if old_header in raw_df_headers:
            raw_df = raw_df.withColumnRenamed(old_header, new_header)

    # Creating a country column.
    cleaned_df = raw_df.withColumn("country", lit(country))
    cleaned_df = cleaned_df.select(["country", "vaccination_type", "vaccination_date"])
    
    final_df = final_df.unionByName(cleaned_df)
    return final_df


def generate_vaccination_count(data_df: DataFrame) -> DataFrame:
    """
    Generate Vaccination Count for each country and
    returns the DataFrame.
    Stores the count data in column 'count'.
    Args:
        data_df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    res_df = data_df.groupBy(["country", "vaccination_type"]).count()
    return res_df


def _get_percentage_vaccinated(population_mapping: dict):
    """
    Creates UDF and computes the vaccine count of each country.
    Args:
        population_mapping (dict): Population details of all countries.
    """

    def _calculate_percentage(country: str, vaccine_count: int) -> float:
        # Fetching country's total population
        total_population = population_mapping[country]
        percentage = float("{:.2f}".format(
            (vaccine_count / total_population) * 100))
        return percentage

    return udf(_calculate_percentage, DoubleType())
# 98.7654 #98.77

def generate_vaccinated_percentage(data_df: DataFrame) -> DataFrame:
    """
    Generate Percentage vaccinated for each country and
    returns the DataFrame.
    Stores the percentage data in column 'vaccinated_percentage'.
    Args:
        data_df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    grouped_df = data_df.groupBy(["country"]).count()
    res_df = grouped_df.withColumn(
        "vaccinated_percentage",
        _get_percentage_vaccinated(COUNTRY_POPULATION)("country", "count"))
    res_df = res_df.drop("count")
    return res_df


def _get_percentage_contribution(world_population: int):
    """
    Creates UDF and computes the vaccinated percentage contribution
    of each country.
    Args:
        world_population (dict): World's Population
    """

    def _calculate_percentage(vaccine_count: int) -> float:
        percentage = float("{:.2f}".format(
            (vaccine_count / world_population) * 100))
        return percentage

    return udf(_calculate_percentage, DoubleType())


def generate_vaccinated_contribution(data_df: DataFrame) -> DataFrame:
    """
    Generate each country's percentage of vaccination contribution of the 
    world's population and returns the DataFrame.
    Stores the percentage data in column 'vaccinated_percentage'.
    Args:
        data_df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    grouped_df = data_df.groupBy(["country"]).count()

    # Calculating world's population.
    total_population = grouped_df.agg({'count': 'sum'}).collect()[0][0]

    res_df = grouped_df.withColumn(
        "vaccinated_contribution",
        _get_percentage_contribution(total_population)("count"))
    res_df = res_df.drop("count")
    return res_df


def init_process():
    """
    Process Input Raw files, apply transformations and
    creates output files of different metrics.
    """
    list_of_input_files = _get_input_files_info(INPUT_DATASETS_DIR)
    spark = _init_spark()
    
    # Creating Final Schema for tranformed DataFrame.
    final_df_schema = StructType() \
                        .add(StructField("country", StringType(), True)) \
                        .add(StructField("vaccination_type", StringType(), True)) \
                        .add(StructField("vaccination_date", StringType(), True))

    # Creating Empty DataFrame with schema.
    final_df = spark.createDataFrame([], schema=final_df_schema)
    for file in list_of_input_files:
        # Extacting file name without extenstion.
        country = os.path.splitext(os.path.basename(file))[0]
        raw_df = read_input_file(spark, file)
        final_df = transform_data(raw_df, final_df, country)

    final_df = transform_date_column(final_df)

    # Generating vaccination count for each country
    # and writing the output to local disk.
    vaccination_count_df = generate_vaccination_count(final_df)
    vaccination_count_df.coalesce(1).write.csv("output_files/vaccination_count",
                                               mode="overwrite",
                                               header=True)

    # Generating vaccinated percentage for each country
    # and writing the output to local disk.
    vaccinated_percentage_df = generate_vaccinated_percentage(final_df)
    vaccinated_percentage_df.coalesce(1).write.csv(
        "output_files/vaccination_percentage", mode="overwrite", header=True)

    # Generating country's vaccinated contribution in the world
    # and writing the output to local disk.
    vaccination_contribution_df = generate_vaccinated_contribution(final_df)
    vaccination_contribution_df.coalesce(1).write.csv(
        "output_files/vaccination_contribution", mode="overwrite", header=True)


if __name__ == "__main__":
    init_process()
