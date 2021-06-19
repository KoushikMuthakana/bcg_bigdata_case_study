from pyspark.sql.functions import col
from utils import Utils, schemas


class TopSpeedingVehicles:
    """
    Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
    has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest
    number of offences (to be deduced from the data)
    """

    def __process(self, session, files):
        """
            process the top 5 vehicles with speeding related offences has licensed drivers

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return:  Returns a : Int
        """
        source_path = files['inputpath']
        charges_use_csv_path = source_path + "/" + files["charges"]

        charges_df = Utils.load_csv(session=session, path=charges_use_csv_path, header=True,
                                    schema=schemas.charges_schema)
        person_use_csv_path = source_path + "/" + files["person"]
        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)
        units_use_csv_path = source_path + "/" + files["units"]
        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)
        join_condition = charges_df.CRASH_ID == person_df.CRASH_ID
        join_type = "inner"
        speeding_with_licences = charges_df.join(person_df, join_condition, join_type).where(
            col("CHARGE").like("%SPEED%")) \
            .select(charges_df.CRASH_ID, charges_df.CHARGE,charges_df.UNIT_NBR, person_df.DRVR_LIC_TYPE_ID, person_df.DRVR_LIC_STATE_ID,
                    person_df.DRVR_LIC_CLS_ID) \
            .where(col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")
        speeding_with_licences.show()

        top_states_offences = person_df.groupBy("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc())
        top_licended_vehicle_colors = units_df.join(person_df, units_df.CRASH_ID == person_df.CRASH_ID, "inner") \
            .where(col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE") \
            .groupBy("VEH_COLOR_ID").count().orderBy(col("count").desc())

        speeding_with_licences.join(top_states_offences, speeding_with_licences.DRVR_LIC_STATE_ID == top_states_offences.DRVR_LIC_STATE_ID, "inner") \
            .groupBy("UNIT_NBR").count()\


    @staticmethod
    def execute(session, files):
        """
             Invokes the process methods to get tha analysis report

        :param session: SparkSession -> Spark Session object
        :param files: Config
        :return: Integer -> Total No of crashes
        """
        return TopSpeedingVehicles.__process(TopSpeedingVehicles, session, files)
