from pyspark.sql.functions import col, count
from utils import Utils, schemas


class TopStatesCrashes:
    """
    Analysis 3: Which state has highest number of accidents in which females are involved?
    """

    def __process(self, session, files):
        """
             Process the highest number of accidents states were females are involved

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]

        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        result = person_df.filter(person_df.PRSN_GNDR_ID == "FEMALE") \
            .groupBy("DRVR_LIC_STATE_ID") \
            .agg(count(col("CRASH_ID")).alias("TotalCrashes")) \
            .orderBy(col("TotalCrashes").desc())\
            .first()
        return result["DRVR_LIC_STATE_ID"] + " - " + str(result["TotalCrashes"])

    @staticmethod
    def execute(session, files):
        """
        Invokes the process methods to get tha analysis report

        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TopStatesCrashes.__process(TopStatesCrashes, session, files)
