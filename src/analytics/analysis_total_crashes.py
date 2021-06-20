from pyspark.sql.functions import col
from utils import Utils, schemas


class AccidentAnalysis:
    """
    Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
    """

    def __process(self, session, files):
        """

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Int

        Sample output:
        +----------------------+
        | TOTAL_CRASHES: 182   |
        +----------------------+

        """
        # Input files path
        source_path = files['inputpath']
        person_use_csv_path = source_path + "/" + files["person"]

        # Loads the files data
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        total_crashes_males = person_df\
            .where((col("PRSN_GNDR_ID") == 'MALE')
                   & (col("DEATH_CNT") > 0))\
            .count()

        return total_crashes_males

    @staticmethod
    def execute(session, files):
        """
                 Invokes the process methods to get tha analysis report
        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return AccidentAnalysis.__process(AccidentAnalysis, session, files)
