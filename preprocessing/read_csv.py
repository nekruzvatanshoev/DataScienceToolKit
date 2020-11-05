

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