import os
import pandas as pd
import json


class LocalHostPersistance:
    date_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self):
        if os.name == 'nt':
            self.local_host_path = "tmp"
        else:
            self.local_host_path = "/tmp"

    def local_file_path(self, tokens):
        all_tokens = [self.local_host_path] + tokens
        return os.path.join(*all_tokens)

    @staticmethod
    def create_directory(dir_path):
        dir_exist = os.path.exists(dir_path)
        if not dir_exist:
            os.makedirs(dir_path)

    def load_json(self, tokens):
        local_file_path = self.local_file_path(tokens)
        with open(local_file_path) as json_file:
            data = json.load(json_file)
        return data

    @staticmethod
    def load_dataframe(file_path):
        df = pd.read_csv(os.path.join(file_path))
        df["Date"] = pd.to_datetime(df["Date"], format=LocalHostPersistance.date_format)
        df.sort_values(by=["Date"], inplace=True)
        df.drop_duplicates(subset="Date", keep='first', inplace=True)
        df.set_index("Date", inplace=True)
        return df
