import argparse
from pyspark.sql import SparkSession, DataFrame
from dependencies import *
from analytics import *

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--pipeline')
    args = parser.parse_args()
    analytics_type = args.pipeline
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
    if isinstance(res,DataFrame):
        res.show()
    else:
        print(f"{str(analytics_type).upper()}: {res}")
