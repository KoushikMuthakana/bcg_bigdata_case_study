from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank
from utils import Utils, schemas


class BodyStyleAnalysis:
    """
    Analysis 5: For all the body styles involved in crashes,
    mention the top ethnic user group of each unique body style
    """

    def __process(self, session, files):
        """
        Process for all the body styles involved in crashes,
            mention the top ethnic user group of each unique body style

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Int

        Sample output:

        |VEH_BODY_STYL_ID                 |PRSN_ETHNICITY_ID|
        +---------------------------------+-----------------+
        |BUS                              |HISPANIC         |
        |NA                               |WHITE            |
        |VAN                              |WHITE            |
        -----------------------------------------------------
        """
        # Input files paths
        source_path = files['inputpath']
        units_use_csv_path = source_path + "/" + files["units"]
        person_use_csv_path = source_path + "/" + files["person"]

        # Reads the CSV files data
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        # Inner Joining  Units with Person on crash_id
        join_condition = units_df.CRASH_ID == person_df.CRASH_ID
        join_type = "inner"

        joined_res = units_df.join(person_df, join_condition, join_type) \
            .select(units_df.CRASH_ID,
                    units_df.VEH_BODY_STYL_ID,
                    person_df.PRSN_ETHNICITY_ID)

        # Total Number of crashes
        top_body_styles = joined_res.\
            groupBy(units_df.VEH_BODY_STYL_ID, person_df.PRSN_ETHNICITY_ID)\
            .count() \
            .orderBy(col("count").desc())

        window = Window.\
            partitionBy("VEH_BODY_STYL_ID")\
            .orderBy(col("count").desc())

        # Top body style of the vehicle involved in the crash
        top_ethnic_user_group = top_body_styles.\
            withColumn("rank", dense_rank().over(window)) \
            .filter("rank= 1")\
            .drop("rank", "count")

        return top_ethnic_user_group

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml `config['files']`
        :return: Integer -> Total No of crashes
        """
        return BodyStyleAnalysis.__process(BodyStyleAnalysis, session, files)
