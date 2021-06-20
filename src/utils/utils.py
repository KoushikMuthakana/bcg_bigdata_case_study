class Utils:

    @staticmethod
    def load_csv(session, path, header, schema, delimiter=","):
        """
        This methods reads the CSV files
        :param session: SparkSession -> ~`pyspark.sql.SparkSession`
        :param path:  String -> CSV file path
        :param header: Boolean -> Header columns
        :param schema: Schema  -> pyspark.sql.types.StructType
        :param delimiter: string -> Default ","
        :return: DataFrame ->     `pyspark.sql.DataFrame`

        """

        data_df = session.read \
            .format("csv") \
            .option("delimiter", delimiter) \
            .schema(schema) \
            .option("header", header) \
            .option("path", path) \
            .load()
        return data_df

    @staticmethod
    def save(dataframe, file_format, output_path):
        """

        :param dataframe:  DataFrame -> `pyspark.sql.DataFrame`
        :param file_format:  String -> Output file format, eg: Parquet,ORC,CSV
        :param output_path: String -> Output path
        :return: None
        """
        dataframe.write \
            .format(file_format) \
            .mode("append")\
            .option("path", output_path) \
            .save()
