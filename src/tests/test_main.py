import unittest
import logging
from pyspark.sql import SparkSession


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_logging(cls):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master("local[2]")
                .appName("my-local-testing-pyspark-context")
                .enableHiveSupport()
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class SparkTest(PySparkTest):
    def test_basic(self):
        test_rdd = self.spark.sparkContext.parallelize(["bcg bigdata use case", "bcg bigdata use case"], 2)
        results = test_rdd.flatMap(lambda line: line.split()) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda x, y: x + y).collect()
        expected_results = [("bcg", 2), ("bigdata", 2), ("use", 2), ("case", 2)]
        self.assertEqual(set(results), set(expected_results))

