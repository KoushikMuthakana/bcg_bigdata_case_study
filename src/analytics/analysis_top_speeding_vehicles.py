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
        :return:  Returns a : Dataframe `~spark.sql.SparkSession`

        Sample output:

        |VEH_MAKE_ID|
        +-----------+
        |       FORD|
        |      HONDA|
        |     SUBARU|
        |    HYUNDAI|
        ------------
        """
        # Input files path
        source_path = files['inputpath']
        charges_use_csv_path = source_path + "/" + files["charges"]
        person_use_csv_path = source_path + "/" + files["person"]
        units_use_csv_path = source_path + "/" + files["units"]

        # Reading from the CSV files
        charges_df = Utils.load_csv(session=session, path=charges_use_csv_path, header=True,
                                    schema=schemas.charges_schema)

        person_df = Utils.load_csv(session=session, path=person_use_csv_path, header=True,
                                   schema=schemas.primary_person_schema)

        units_df = Utils.load_csv(session=session, path=units_use_csv_path, header=True,
                                  schema=schemas.units_schema)

        # Joining on charges data with person data on crash_id
        join_condition = charges_df.CRASH_ID == person_df.CRASH_ID
        join_type = "inner"

        # charges with speed related offences with driver licences
        speeding_with_licences = charges_df.join(person_df, join_condition, join_type)\
            .where((col("CHARGE").like("%SPEED%")) &
                   (col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")) \
            .select(charges_df.CRASH_ID,
                    charges_df.CHARGE,
                    charges_df.UNIT_NBR,
                    person_df.DRVR_LIC_TYPE_ID,
                    person_df.DRVR_LIC_STATE_ID,
                    person_df.DRVR_LIC_CLS_ID
                    )

        # Top states with highest number of offences
        top_states_offences = person_df\
            .groupBy("DRVR_LIC_STATE_ID")\
            .count()\
            .orderBy(col("count").desc()).take(25)

        # Top used vehicles colours with licenced
        top_licensed_vehicle_colors = units_df.\
            join(person_df, units_df.CRASH_ID == person_df.CRASH_ID, "inner") \
            .where((col("DRVR_LIC_TYPE_ID") == "DRIVER LICENSE")
                   & (col("DRVR_LIC_CLS_ID").like("CLASS C%"))) \
            .groupBy("VEH_COLOR_ID")\
            .count()\
            .orderBy(col("count").desc()).take(10)

        top_colors = [i["VEH_COLOR_ID"] for i in top_licensed_vehicle_colors]
        top_states = [i['DRVR_LIC_STATE_ID'] for i in top_states_offences]

        top_vehicles_made = speeding_with_licences\
            .join(units_df, speeding_with_licences.CRASH_ID == units_df.CRASH_ID, "inner") \
            .where((col("VEH_COLOR_ID").isin(top_colors))
                   & (col("DRVR_LIC_STATE_ID").isin(top_states))) \
            .select("VEH_MAKE_ID")

        return top_vehicles_made

    @staticmethod
    def execute(session, files):
        """
             Invokes the process methods to get tha analysis report

        :param session: SparkSession : `~pyspark.sql.SparkSession`
        :param files: Yaml config['files']
        :return: Dataset `~pyspark.sql.dataframe` -> Top Vehicles colors
        """
        return TopSpeedingVehicles.__process(TopSpeedingVehicles, session, files)
