from test_main import PySparkTest
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from utils.utils import Utils


class UtilsTest(PySparkTest):

    def test_load_csv(self):
        session = self.spark
        path = "./test_files/test.csv"
        schema = StructType([
            StructField("CRASH_ID", IntegerType()),
            StructField("UNIT_NBR", IntegerType()),
            StructField("UNIT_DESC_ID", StringType())
        ])

        test_df = Utils.load_csv(session=session, path=path, header=True, schema=schema)
        self.assertEqual(5, test_df.count())

    def test_save(self):
        testDf = self.spark.createDataFrame([("bcg", 3), ("bigdata", 2)]).toDF("name", "rank")
        msg = Utils.save(testDf, "csv", "test")
        self.assertEqual("Success", msg)
