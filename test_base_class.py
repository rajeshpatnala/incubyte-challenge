import unittest
from pyspark.sql import SparkSession


class PysparkTestBaseClass(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.spark = SparkSession \
                     .builder \
                     .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.4") \
                     .config("spark.sql.shuffle.partitions", "4") \
                     .master("local[2]") \
                     .appName("Unit-tests") \
                     .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
