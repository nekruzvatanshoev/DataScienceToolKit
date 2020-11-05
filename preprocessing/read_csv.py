

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

class PrePRocessCSVPandas(AbstractPreProcessCSV):
    def __init__(self, data):
        """

        :param data:
        """
        super(PrePRocessCSVPandas, self).__init__()
        self.data = data

    def get_size(self):
        """

        :return:
        """
        return self.data.shape

    def remove_col_by_name(self, column_name):
        """

        :return:
        """
        assert type(column_name) == str
        del self.data[column_name]

    def add_col_by_name(self, column_name, input_data):
        assert len(input_data) == len(self.data.index)
        self.data[column_name] = input_data


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
        pass

    def to_list(self):
        return self.data.toPandas().values.tolist()

class PreProcessCSV:

    def __init__(self, data):
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
        self.data[column_name] = input_data

    def to_list(self):
        """
        Converts a Pandas DataFrame into a list of lists
        :return: A list of lists
        """
        return self.data.values.tolist()

