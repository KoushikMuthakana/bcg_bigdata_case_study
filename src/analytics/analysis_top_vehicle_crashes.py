from pyspark.sql import Window
from pyspark.sql.functions import col, sum, desc, dense_rank
from utils import Utils, schemas


class TopVehicleCrashes:
    """
    Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that
    contribute to a largest number of injuries including death
    """

    def __process(self, session, files):
        """

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Dataframe

        Sample output
        +---------------+-----------+
        |    VEH_MAKE_ID|TOTAL_DEATHS|
        +---------------+-----------+
        |         TOYOTA|         20|
        |          HONDA|         17|
        |HARLEY-DAVIDSON|         15|
        |          DODGE|         12|
        |         SUZUKI|          8|
        |       CHRYSLER|          6|
        -----------------------------
        """
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]

        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        top_vehicles_crashes = units_df.groupBy("VEH_MAKE_ID") \
            .agg(sum("DEATH_CNT").alias("totaldeaths")) \
            .orderBy(col("TOTAL_DEATHS"))

        window = Window.orderBy(desc("TOTAL_DEATHS"))

        top_range = top_vehicles_crashes. \
            withColumn("id", dense_rank().over(window)) \
            .filter((col("id") >= 5) & (col("id") <= 15)).drop("id")

        top_range.show()

        return top_range

    @staticmethod
    def execute(session, files):
        """
         Invokes the process methods to get tha analysis report

        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TopVehicleCrashes.__process(TopVehicleCrashes, session, files)
