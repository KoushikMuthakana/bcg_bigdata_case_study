import argparse
from pyspark.sql import SparkSession, DataFrame
from utils import Utils
from dependencies import files
from analytics import AccidentAnalysis, TwoWheelersAnalysis, TopStatesCrashes, TopVehicleCrashes, BodyStyleAnalysis, \
    TopZipCodes, SafeCrashes, TopSpeedingVehicles

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Big Data Case Study")
    parser.add_argument('--pipeline', help="provide the pipeline", default="total_crashes", )
    parser.add_argument('--output_file_path', help="output file path", default="/")
    parser.add_argument('--output_format', help="output file format", default="parquet")
    args = parser.parse_args()
    analytics_type = args.pipeline
    output_path = args.output_file_path
    output_file_format = args.output_format
    pipelines = {
        "total_crashes": AccidentAnalysis,
        "total_two_wheelers_crashes": TwoWheelersAnalysis,
        "top_states_crashes": TopStatesCrashes,
        "top_vehicles_crashes": TopVehicleCrashes,
        "body_style_crashes": BodyStyleAnalysis,
        "top_zip_codes": TopZipCodes,
        "safe_crashes": SafeCrashes,
        "top_speeding": TopSpeedingVehicles
    }

    spark = SparkSession \
        .builder \
        .appName("BCG Accidents Analytics Use case") \
        .getOrCreate()

    res = pipelines[analytics_type].execute(session=spark, files=files)
    if isinstance(res, DataFrame):
        Utils.save(res, file_format=output_file_format, output_path=output_file_format)
    else:
        print(f"{str(analytics_type).upper()}: {res}")
