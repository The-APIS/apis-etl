from sayn import PythonTask
from google.cloud import storage

class getData(PythonTask):

    def run(self):

        bucket_name = self.parameters["bucket"]
        source_blob_name = self.parameters["object"]
        destination_file_name = self.parameters["path"]

        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        self.info(f"Blob {source_blob_name} downloaded to {destination_file_name}")

        return self.success()
