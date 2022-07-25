"""
Script to create requried base class for the testing of process script.
This module has setUpClass and TearDownClass methods which helps to setup
SparkSession and closes it once the testing is completed.

This Module download the required dependency jar files for following package
com.crealytics:spark-excel_2.12:0.13.4. This jar is used to read microsoft excel files.
"""
import unittest
from pyspark.sql import SparkSession


class PysparkTestBaseClass(unittest.TestCase):
    """
    This Class contains method whic creates the SparkSession
    and stops it.
    """

    @classmethod
    def setUpClass(cls):
        """
        Creates the SparkSession.
        """
        cls.spark = SparkSession \
                     .builder \
                     .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.4") \
                     .config("spark.sql.shuffle.partitions", "4") \
                     .master("local[2]") \
                     .appName("Unit-tests") \
                     .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """
        Stops the SparkSession.
        """
        cls.spark.stop()
