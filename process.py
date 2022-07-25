import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import *
from constants import INPUT_DATASETS_DIR, COUNTRY_POPULATION


def _get_input_files_info(input_path) -> list:
    _input_data_files = []
    for file_name in os.listdir(input_path):
        _input_data_files.append(os.path.join(input_path, file_name))
    return _input_data_files


def _init_spark():
    spark = SparkSession \
                .builder \
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.4") \
                .config("spark.sql.shuffle.partitions", "4") \
                .master("local[2]") \
                .appName("spark-session") \
                .getOrCreate()
    return spark


def read_input_file(spark: SparkSession, input_file: str) -> DataFrame:
    file_ext = os.path.splitext(input_file)[1]
    data_df = spark.createDataFrame([], StructType([]))
    if file_ext == ".csv":
        data_df = spark.read \
                       .option("inferSchema", "true") \
                       .option("header", "true") \
                       .csv(input_file)
    elif file_ext == ".xlsx":
        data_df = spark.read \
                       .format("com.crealytics.spark.excel") \
                       .option("header", "true") \
                       .option("inferSchema", "true") \
                       .load(input_file)
    return data_df


def transform_data(raw_df: DataFrame, final_df: DataFrame,
                   country: str) -> DataFrame:

    raw_df_headers = raw_df.columns
    for old_header, new_header in zip(["VaccinationType", "Vaccine Type"],
                                      ["vaccination_type", "vaccination_type"]):
        if old_header in raw_df_headers:
            raw_df = raw_df.withColumnRenamed(old_header, new_header)

    cleaned_df = raw_df.withColumn("country", lit(country))
    cleaned_df = cleaned_df.select(["country", "vaccination_type"])
    final_df = final_df.unionByName(cleaned_df)
    return final_df


def generate_vaccination_count(data_df: DataFrame) -> DataFrame:

    res_df = data_df.groupBy(["country", "vaccination_type"]).count()
    return res_df


def _get_percentage_vaccinated(population_mapping: dict):

    def _calculate_percentage(country: str, vaccine_count: int) -> float:
        total_population = population_mapping[country]
        percentage = float("{:.2f}".format(
            (vaccine_count / total_population) * 100))
        return percentage

    return udf(_calculate_percentage, DoubleType())


def generate_vaccinated_percentage(data_df: DataFrame) -> DataFrame:
    grouped_df = data_df.groupBy(["country"]).count()
    res_df = grouped_df.withColumn(
        "vaccinated_percentage",
        _get_percentage_vaccinated(COUNTRY_POPULATION)("country", "count"))
    res_df = res_df.drop("count")
    return res_df


def _get_percentage_contribution(total_population: int):

    def _calculate_percentage(vaccine_count: int) -> float:
        percentage = float("{:.2f}".format(
            (vaccine_count / total_population) * 100))
        return percentage

    return udf(_calculate_percentage, DoubleType())


def generate_vaccinated_contribution(data_df: DataFrame) -> DataFrame:

    total_population = data_df.groupBy(["Country"]).count().agg({
        'count': 'sum'
    }).collect()[0][0]
    grouped_df = data_df.groupBy(["country"]).count()
    res_df = grouped_df.withColumn(
        "vaccinated_contribution",
        _get_percentage_contribution(total_population)("count"))
    res_df = res_df.drop("count")
    return res_df


def init_process():

    list_of_input_files = _get_input_files_info(INPUT_DATASETS_DIR)
    spark = _init_spark()
    final_df_schema = StructType() \
                        .add(StructField("country", StringType(), True)) \
                        .add(StructField("vaccination_type", StringType(), True))
    final_df = spark.createDataFrame([], schema=final_df_schema)
    for file in list_of_input_files:
        file_name_with_ext = os.path.basename(file)
        country = os.path.splitext(file_name_with_ext)[0]
        raw_df = read_input_file(spark, file)
        final_df = transform_data(raw_df, final_df, country)

    vaccination_count_df = generate_vaccination_count(final_df)
    vaccination_count_df.coalesce(1).write.csv("output_files/vaccination_count",
                                               mode="overwrite",
                                               header=True)
    vaccinated_percentage_df = generate_vaccinated_percentage(final_df)
    vaccinated_percentage_df.coalesce(1).write.csv(
        "output_files/vaccination_percentage", mode="overwrite", header=True)
    vaccination_contribution_df = generate_vaccinated_contribution(final_df)
    vaccination_contribution_df.coalesce(1).write.csv(
        "output_files/vaccination_contribution", mode="overwrite", header=True)


if __name__ == "__main__":
    init_process()
