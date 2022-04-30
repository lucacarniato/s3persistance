import boto3
from io import StringIO, BytesIO
import os
import json
import joblib
import pandas as pd

from LocalHostPersistance import LocalHostPersistance


class S3Persistance:
    """This class can be used to download data from S3 to a local temporary folder or upload files to S3.
    Files are stored in a folder named that mirrors the S3 bucket structure.
    """

    def __init__(self, persistance_bucket: str):
        """S3Persistance constructor

        Args:
            persistance_bucket (str): The bucket name
        """

        self.session = boto3.Session()
        self.s3_resource = self.session.resource('s3')
        self.s3_client = self.session.client("s3")
        self.persistance_bucket = persistance_bucket
        self.local_host_persistance = LocalHostPersistance()

    @staticmethod
    def key_val_from_bucket_path(bucket_path: str):
        """Returns the key and value from a bucket path.

        Args:
            bucket_path (str): The bucket path.
        Returns:
            key: The bucket key
            value: The bucket value
        """
        dirs = bucket_path.split("/")
        if len(dirs) == 1:
            key = "/"
            value = dirs[0]
            return key, value
        key = ""
        for dir in dirs[:-1]:
            key += dir + "/"
        value = dirs[-1]
        return key, value

    @staticmethod
    def bucket_path_from_tokens(tokens: list):
        """From a series of tokens compute the bucket path

        Args:
            tokens (list): A list of tokens
        Returns:
            bucket_path: the bucket path
        """
        bucket_path = "/".join(tokens)
        return bucket_path

    def check_if_file_exist(self, bucket_path: str):
        """Check if a file exists in the bucket.

        Args:
            bucket_path (str): The bucket path
        Returns:
            True if the file exists, False otherwise
        """
        key, val = self.key_val_from_bucket_path(bucket_path)
        if key == "/":
            all_obj = self.s3_resource.Bucket(self.persistance_bucket).objects.all()
        else:
            all_obj = self.s3_resource.Bucket(self.persistance_bucket).objects.filter(Prefix=key)
        for o in all_obj:
            if o.key == bucket_path:
                return True
        return False

    def download_one_file(self, bucket_path):
        """Downloads a file from the bucket to the local temporary folder.

        Args:
            bucket_path (str): The bucket path.
        Returns:
            True if the file was downloaded, False otherwise
        """
        key, val = self.key_val_from_bucket_path(bucket_path)
        local_file_path = self.local_host_persistance.local_file_path(tokens=[key, val])
        local_file_dir = self.local_host_persistance.local_file_path(tokens=[key])
        LocalHostPersistance.create_directory(local_file_dir)
        if os.path.isfile(local_file_path):
            os.remove(local_file_path)
        if self.check_if_file_exist(bucket_path):
            self.s3_client.download_file(self.persistance_bucket,
                                         bucket_path,
                                         local_file_path)
            return True
        return False

    def download_all_files_in_key(self, key):
        """Downloads all files in a bucket key to the local temporary folder.

        Args:
            key (str): The bucket key.
        """
        all_files = self.s3_resource.Bucket(self.persistance_bucket).objects.filter(Prefix=key + "/")
        for f in all_files:
            bucket_path = f.key
            if f.key == key + "/":
                continue
            tokens = bucket_path.split("/")
            local_file_path = self.local_host_persistance.local_file_path(tokens=tokens)
            local_file_dir = self.local_host_persistance.local_file_path(tokens=tokens[:-1])
            LocalHostPersistance.create_directory(local_file_dir)
            self.s3_client.download_file(self.persistance_bucket, bucket_path, local_file_path)

    def delete_all_files_in_key(self, key):
        """Deletes all files in a bucket key.

        Args:
            key (str): The bucket key.
        """
        all_files = self.s3_resource.Bucket(self.persistance_bucket).objects.filter(Prefix=key + "/")
        for f in all_files:
            bucket_path = f.key
            if f.key == key + "/":
                continue
            self.delete_file(bucket_path)

    def delete_file(self, bucket_path):
        """Deletes a single file in the bucket.

        Args:
            bucket_path (str): The file bucket path.
        """
        self.s3_resource.Object(self.persistance_bucket, bucket_path).delete()

    def write_csv(self, bucket_path, df, host_delete_existing_file=True):
        """Writes a csv file to bucket

        Args:
            bucket_path (str): The file bucket path.
        """
        if host_delete_existing_file:
            self.delete_file(bucket_path)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        self.s3_resource.Object(self.persistance_bucket, bucket_path).put(Body=csv_buffer.getvalue())

    def write_json(self, bucket_path, data, host_delete_existing_file=True):
        """Writes a json file to bucket

        Args:
            bucket_path (str): The file bucket path.
        """
        if host_delete_existing_file and self.check_if_file_exist(bucket_path):
            self.delete_file(bucket_path)
        self.s3_resource.Object(self.persistance_bucket, bucket_path).put(Body=bytes(json.dumps(data).encode('UTF-8')))

    def read_json_joblib(self, bucket_path):
        """Reads a json file from bucket, no temporary local file is created

        Args:
            bucket_path (str): The file bucket path.
        Returns:
            The file read as a dictionary.
        """
        if not self.check_if_file_exist(bucket_path):
            raise Exception("read_json_joblib file not found " + bucket_path)
        with BytesIO() as f:
            self.s3_client.download_fileobj(Bucket=self.persistance_bucket, Key=bucket_path, Fileobj=f)
            f.seek(0)
            data = json.load(f)
            return data

    def write_joblib(self, bucket_path, data, host_delete_existing_file=True):
        """Writes a file to bucket, as a byte stream

        Args:
            bucket_path (str): The file bucket path.
        """
        if host_delete_existing_file and self.check_if_file_exist(bucket_path):
            self.delete_file(bucket_path)
        with BytesIO() as f:
            joblib.dump(data, f)
            f.seek(0)
            self.s3_client.upload_fileobj(Bucket=self.persistance_bucket, Key=bucket_path, Fileobj=f)

    def read_bytes_joblib(self, bucket_path):
        """Reads a file from bucket, no temporary local file is created

        Args:
            bucket_path (str): The file bucket path.
        Returns:
            The file read
        """
        if not self.check_if_file_exist(bucket_path):
            raise Exception("read_bytes_joblib file not found " + bucket_path)
        with BytesIO() as f:
            self.s3_client.download_fileobj(Bucket=self.persistance_bucket, Key=bucket_path, Fileobj=f)
            f.seek(0)
            data = joblib.load(f)
            return data

    def read_csv_joblib(self, bucket_path):
        """Reads a csv file, no temporary local file is created

        Args:
            bucket_path (str): The file bucket path.
        Returns:
            The file read as pandas dataframe
        """
        if not self.check_if_file_exist(bucket_path):
            raise Exception("read_csv_joblib file not found " + bucket_path)
        with BytesIO() as f:
            self.s3_client.download_fileobj(Bucket=self.persistance_bucket, Key=bucket_path, Fileobj=f)
            f.seek(0)
            df = pd.read_csv(f)
            return df

    def write_all_dfs(self, dict_df, persistance_destination_key, file_name):
        """Writes a dictionary of dataframes to bucket. Each key corresponds to the bucket path.
        The same file name is used for all dataframes.

        Args:
            dict_df (dict): A dictionary of dataframes
            persistance_destination_key (str): The main bucket
            file_name (str): The name of the file
        """
        for k, v in dict_df.items():
            if len(v) == 0:
                continue
            file_path = self.bucket_path_from_tokens([persistance_destination_key, k, file_name])
            self.write_csv(file_path, v)

    def copy_bucket_files_by_extension(self, source_bucket, extension, bucket_source_key, persistance_destination_key):
        """Writes a dictionary of dataframes to bucket. Each key corresponds to the bucket path.
        The same file name is used for all dataframes.

        Args:
            source_bucket (dict): The source bucket
            extension (str): The file extension of the files to copy
            bucket_source_key (str): The destination bucket
            persistance_destination_key (str): The destination bucket key
        """
        all_obj = self.s3_resource.Bucket(source_bucket).objects.filter(Prefix=bucket_source_key + "/")
        for f in all_obj:
            copy_source = {'Bucket': source_bucket, 'Key': f.key}
            file_name = f.key.split("/")[-1]
            file_without_dir = f.key.split("/")[1:]
            if file_name.endswith(extension):
                self.s3_resource.meta.client.copy(copy_source,
                                                  self.persistance_bucket,
                                                  S3Persistance.bucket_path_from_tokens(tokens=[persistance_destination_key] + file_without_dir))
