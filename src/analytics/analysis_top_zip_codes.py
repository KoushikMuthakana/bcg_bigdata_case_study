from pyspark.sql.functions import col
from utils import Utils, schemas


class TopZipCodes:
    """
    Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the
    contributing factor to a crash (Use Driver Zip Code)
    """

    def __process(self, session, files):
        """

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Dataframe

        Sample output:

            +--------+-----+
            |DRVR_ZIP|count|
            +--------+-----+
            |   78521|   80|
            |   76010|   66|
            +--------+-----+
        """
        # Input files path
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]
        units_use_csv_path = source_path + "/" + files["units"]

        # Loads the file data
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        # Dropping all the null values in Driver zip columns
        valid_person_df = person_df.na.drop(subset=["DRVR_ZIP"])

        join_condition = units_df.CRASH_ID == person_df.CRASH_ID
        join_type = "inner"

        top_zipcode_crashes = units_df.join(valid_person_df, join_condition, join_type) \
            .where(
            "VEH_BODY_STYL_ID in ('PASSENGER CAR, 4-DOOR', 'SPORT UTILITY VEHICLE', 'PASSENGER CAR, 2-DOOR') and  "
            "PRSN_ALC_RSLT_ID = 'Positive' "
        ).groupBy("DRVR_ZIP")\
            .count()\
            .orderBy(col("count").desc()).take(5)

        return session.createDataFrame(top_zipcode_crashes).show()

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report

            :param session: SparkSession -> Spark Session object
            :param files: Config
            :return: Dataframe
            """
        return TopZipCodes.__process(TopZipCodes, session, files)
