import os
import pandas as pd


class LocalHostPersistance:
    """Class wrapping the operating system specific tmp file directory and containing the methods for
    creating directories and loading files.
    """

    def __init__(self):
        if os.name == 'nt':
            self.local_host_path = "tmp"  # Windows tmp directory
        else:
            self.local_host_path = "/tmp" # Linux tmp directory

    @staticmethod
    def create_directory(dir_path):
        """Creates a directory at the specified path if it does not exist.

        Args:
            dir_path (str): The directory path
        """
        dir_exist = os.path.exists(dir_path)
        if not dir_exist:
            os.makedirs(dir_path)

    @staticmethod
    def load_dataframe(file_path):
        """Loads a dataframe stored in the local file system.

        Args:
            file_path (str): The dataframe path
        """
        date_format = "%Y-%m-%d %H:%M:%S"
        df = pd.read_csv(os.path.join(file_path))
        df["Date"] = pd.to_datetime(df["Date"], format=date_format)
        df.sort_values(by=["Date"], inplace=True)
        df.drop_duplicates(subset="Date", keep='first', inplace=True)
        df.set_index("Date", inplace=True)
        return df

    def local_file_path(self, tokens):
        """From a list of tokens, creates a local path in the tmp.

        Args:
            tokens (list): The list of tokens that will be used to create the file path.
        """
        all_tokens = [self.local_host_path] + tokens
        return os.path.join(*all_tokens)