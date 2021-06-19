class Utils:

    @staticmethod
    def load_csv(session, path, header, schema, delimiter=","):
        """

        :param session:
        :param path:
        :param header:
        :param schema:
        :param delimiter:
        :return:
        """
        df = session.read\
            .format("csv")\
            .option("delimiter",delimiter)\
            .schema(schema)\
            .option("header", header)\
            .option("path", path)\
            .load()
        return df
