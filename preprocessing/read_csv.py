import pandas as pd

class ReadCSV:
    def __init__(self, filepath):
        """

        :param filepath:
        """
        self.filepath = filepath
        self.data = pd.read_csv(filepath_or_buffer=filepath)

    def get_size(self):
        """
        Return the number of rows and columns within Pandas DataFrame
        :return:
        """
        return self.data.shape

    def remove_col_by_name(self, column_name):
        """
        Remove a column from Pandas DataFrame by name
        :param column_name: The name of the column we would like to remove
        :return:
        """
        assert type(column_name) == str
        del self.data[column_name]

    def add_col_by_name(self, column_name, input_data):
        """
        Add a new column with new data
        :param column_name: The name of the column we would like to add
        :param input_data: The input data array
        :return:
        """
        assert len(input_data) == len(self.data.index)
        self.data[column_name] = input_data

    def to_list(self):
        """
        Convert Pandas DataFrame into a list of lists
        :return:
        """
        return self.data.values.tolist()