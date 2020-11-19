

class AbstractPreProcessCSV:
    def __init__(self):
        pass

    def get_size(self):
        raise NotImplementedError

    def remove_col_by_name(self, column_name):
        raise NotImplementedError

    def add_col_by_name(self, column_name, input_data):
        raise NotImplementedError

    def to_list(self):
        pass

    def to_csv(self, file_name):
        raise NotImplementedError


class PreProcessCSVSpark(AbstractPreProcessCSV):
    def __init__(self, data):
        super(PreProcessCSVSpark, self).__init__()
        self.data = data

    def get_size(self):
        """

        :return:
        """
        return self.data.shape


    def remove_cols_by_name(self, column_names):
        self.data = self.data.drop(columns=column_names)


    def add_col_by_name(self, column_name, input_data):
        """

        :param column_name:
        :param input_data:
        :return:
        """
        pass

    def to_list(self):
        """

        :return:
        """
        return self.data.toPandas().values.tolist()

    def to_show(self, top_n=10, truncate=False, vertical=False):
        """
        Displays the DataFrame vertically with rows in the first column being the columns in the DataFrame
        and the values in the second individual record
        :param top_n: Display the top n rows (records)
        :param truncate:
        :param vertical:
        :return:
        """
        self.data.show(top_n, truncate=truncate, vertical=vertical)

    def to_csv(self, file_name):
        self.data.write.csv(file_name=file_name)

class PreProcessCSVPandas(AbstractPreProcessCSV):

    def __init__(self, data):
        super().__init__()
        """
        import pandas as pd

        data = pd.read_csv("some_csv_dataset.csv")
        PreProcessCSV(data)

        :param data: Incoming Pandas DataFrame
        """
        self.data = data

    def get_size(self):
        """
        Returns the number of rows and columns within Pandas DataFrame
        :return: A tuple (rows, columns)
        """
        return self.data.shape

    def remove_col_by_name(self, column_name):
        """
        Maybe column_name can be also a list and thus we can drop a list of columns
        Maybe we can perform similar procedure with
        Removes a column from Pandas DataFrame by name
        :param column_name: The name of the column we would like to remove
        :return: None
        """
        assert type(column_name) == str
        del self.data[column_name]

    def add_col_by_name(self, column_name, input_data):
        """
        Adds a new column with new data
        :param column_name: The name of the column we would like to add
        :param input_data: The input data array
        :return: None
        """
        assert len(input_data) == len(self.data.index)

        num_cols = len(self.data.columns) - 1
        self.data.insert(loc=num_cols, column=column_name, value=input_data)


    def to_list(self):
        """
        Converts a Pandas DataFrame into a list of lists
        :return: A list of lists
        """
        return self.data.values.tolist()

    def to_csv(self, file_name):
        self.data.to_csv(file_name=file_name)

# from pyspark.sql import SparkSession
#
#
# spark = SparkSession.builder.appName("data_processing").getOrCreate()
# import pyspark.sql.functions as F
# from pyspark.sql.types import StructType, FloatType
# schema = StructType().add("user_id", "string")\
#     .add("country", "string").add("browser", "string") \
#     .add("OS", "string").add("age", "integer")
#
#
# df = spark.createDataFrame([("A203", "India", "Chrome", "WIN", 33),
#                             ("A201", "China", "Safari", "MacOS", 35),
#                             ("A205", "UK", "Mozilla", "Linux", 25)], schema=schema)
# df.printSchema()
# df.show()
# df = df.drop('OS')
# df.show()
# data = PreProcessCSVSpark(df)
# print(data.to_list())
#
# new_schema = StructType().add("distance", "integer").add("height", "integer")
# print(new_schema)
#
# distance = [10, 15, 20]
# new_df = spark.createDataFrame([
#     (10,1),(15,2),(20,3)
# ], schema=new_schema)
#
# df = df.withColumn()
# df.show()


# import pandas as pd
#
# data = pd.read_csv("/home/nekruz/pythonProject/PythonOOP/train.csv")
# df = PreProcessCSVPandas(data)
# print(df.data)
# df.remove_col_by_name("R")
# print(df.data)
# arr = [x for x in range(106)]
# df.add_col_by_name("NEW", arr)
# print(df.data)
# print(df.to_list())