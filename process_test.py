"""
Script to automate the testing for process script.
This script uses test files and generates end results,
compares it with expected files.

Dependency Test files:
1. test_files directory. 

Dependency Python files:
1. test_base_class.py
2. constansts.py
3. process.py
"""

import os
from pyspark.sql.types import *
from test_base_class import PysparkTestBaseClass

from constants import (TEST_RAW_DATASETS_DIR, TEST_DATASETS_DIR,
                       TEST_EXPECTED_DATASETS_DIR, TEST_DATASET_CONFIG)

from process import (read_input_file, transform_data,
                     generate_vaccination_count, generate_vaccinated_percentage,
                     generate_vaccinated_contribution)


class PysparkTest(PysparkTestBaseClass):
    """
    This module is used to test process script data processing.
    It will generate end results for given test input files
    and comapre it with expected results.
    """

    def test_read_input_file(self):
        """
        This method tests results generated using read_input_file function
        against expected results.
        
        Test files used:
        test_files/input_data/raw_files

        Expected files used:
        test_files/expected_files/merged_data.csv
        """
        for conf in TEST_DATASET_CONFIG:
            test_file = os.path.join(TEST_RAW_DATASETS_DIR, conf["FILE_NAME"])
            actual_df = read_input_file(self.spark, test_file)
            actual_df_row_count = actual_df.count()
            expected_rows_count = conf["FILE_ROWS"]
            self.assertEqual(expected_rows_count, actual_df_row_count)

    def test_transform_data(self):
        """
        This method tests results generated using transform_data function
        against expected results.
        
        Test files used:
        test_files/input_data/raw_files

        Expected files used:
        test_files/expected_files/merged_data.csv
        """
        merged_data_file = os.path.join(TEST_EXPECTED_DATASETS_DIR,
                                        "merged_data.csv")
        expected_df = self.spark.read.option("header",
                                             True).csv(merged_data_file)

        raw_file = os.path.join(TEST_RAW_DATASETS_DIR, "IND.csv")
        raw_df = self.spark.read.option("header", True).csv(raw_file)
        schema = StructType() \
                        .add(StructField("country", StringType(), True)) \
                        .add(StructField("vaccination_type", StringType(), True))
        actual_df = self.spark.createDataFrame([], schema=schema)
        actual_df = transform_data(raw_df, actual_df, "IND")

        self.assertEqual(sorted(expected_df.collect()),
                         sorted(actual_df.collect()))

    def test_vaccination_count(self):
        """
        This method tests results generated using generate_vaccination_count
        function against expected results.
        
        Test files used:
        test_files/input_data/transformed.csv

        Expected files used:
        test_files/expected_files/vaccination_count.csv
        """
        expected_file = os.path.join(TEST_EXPECTED_DATASETS_DIR,
                                     "vaccination_count.csv")
        expected_df = self.spark.read.option("inferSchema", "true").option(
            "header", True).csv(expected_file)

        raw_file = os.path.join(TEST_DATASETS_DIR, "transformed.csv")
        raw_df = self.spark.read.option("header", True).csv(raw_file)
        actual_df = generate_vaccination_count(raw_df)
        self.assertEqual(sorted(expected_df.collect()),
                         sorted(actual_df.collect()))

    def test_vaccinated_percentage(self):
        """
        This method tests results generated using generate_vaccinated_percentage
        function against expected results.
        
        Test files used:
        test_files/input_data/transformed.csv

        Expected files used:
        test_files/expected_files/vaccination_percentage.csv
        """
        expected_file = os.path.join(TEST_EXPECTED_DATASETS_DIR,
                                     "vaccination_percentage.csv")
        expected_df = self.spark.read.option("inferSchema", "true").option(
            "header", True).csv(expected_file)

        raw_file = os.path.join(TEST_DATASETS_DIR, "transformed.csv")
        raw_df = self.spark.read.option("header", True).csv(raw_file)
        actual_df = generate_vaccinated_percentage(raw_df)
        self.assertEqual(sorted(expected_df.collect()),
                         sorted(actual_df.collect()))

    def test_vaccinated_contribution(self):
        """
        This method tests results generated using generate_vaccinated_contribution
        function against expected results.
        
        Test files used:
        test_files/input_data/transformed.csv

        Expected files used:
        test_files/expected_files/vaccination_contributed.csv
        """
        expected_file = os.path.join(TEST_EXPECTED_DATASETS_DIR,
                                     "vaccination_contributed.csv")
        expected_df = self.spark.read.option("inferSchema", "true").option(
            "header", True).csv(expected_file)

        raw_file = os.path.join(TEST_DATASETS_DIR, "transformed.csv")
        raw_df = self.spark.read.option("header", True).csv(raw_file)
        actual_df = generate_vaccinated_contribution(raw_df)
        self.assertEqual(sorted(expected_df.collect()),
                         sorted(actual_df.collect()))
