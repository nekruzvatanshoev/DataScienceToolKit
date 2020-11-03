import pandas as pd

class ReadCSV:
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = pd.read_csv(filepath_or_buffer=filepath)

    def get_size(self):
        return self.data.shape

    def remove_col_by_name(self, column_name):
        """

        :param column_name: The column name we would like to remove
        :return:
        """
        assert type(column_name) == str
        del self.data[column_name]

    def add_col_by_name(self, column_name, input_data):
        """

        :param column_name: The column name for our new column
        :param input_data: The input data array
        :return:
        """
        assert len(input_data) == len(self.data.index)
        self.data[column_name] = input_data
