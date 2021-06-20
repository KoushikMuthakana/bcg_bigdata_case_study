import argparse
from pyspark.sql import SparkSession, DataFrame
from utils import Utils
from dependencies import files
from utils.logger import logger
from analytics import AccidentAnalysis, TwoWheelersAnalysis, TopStatesCrashes, TopVehicleCrashes, BodyStyleAnalysis, \
    TopZipCodes, SafeCrashes, TopSpeedingVehicles

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Big Data Case Study")
    parser.add_argument('--pipeline', help="provide the pipeline", default="total_crashes", )
    parser.add_argument('--output_file_path', help="output file path", default="/")
    parser.add_argument('--output_format', help="output file format", default="parquet")

    args = parser.parse_args()
    analytics_type = args.pipeline
    output_path = analytics_type if args.output_file_path == '/' else args.output_file_path
    output_file_format = args.output_format

    logger.debug("Started the pipeline - %s ", str(analytics_type).upper())

    pipelines = {
        "total_crashes": AccidentAnalysis,
        "total_two_wheelers_crashes": TwoWheelersAnalysis,
        "top_state_crashes": TopStatesCrashes,
        "top_vehicles_crashes": TopVehicleCrashes,
        "body_style_crashes": BodyStyleAnalysis,
        "top_zip_codes_crashes": TopZipCodes,
        "safe_crashes": SafeCrashes,
        "top_speeding_vehicles_crashes": TopSpeedingVehicles
    }
    try:
        spark = SparkSession \
            .builder \
            .config("spark.app.name", "BCG Accidents Analytics Use case") \
            .getOrCreate()

        # Selects the pipeline and starts processing
        result = pipelines[analytics_type].execute(session=spark, files=files)

        if isinstance(result, DataFrame):
            Utils.save(result, file_format=output_file_format, output_path=output_path)
        else:
            print(f"{str(analytics_type).upper()}: {result}")

    except Exception as err:
        logger.error("%s Error : %s", __name__, str(err))

    finally:
        spark.stop()
        logger.debug("Successfully completed the pipeline - %s ", str(analytics_type).upper())
        logger.debug("Successfully stopped spark session ")
