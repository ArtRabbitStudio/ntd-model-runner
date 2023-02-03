import os, sys
from google.cloud import storage
from google.api_core import exceptions
from google.auth.exceptions import DefaultCredentialsError

class gcs:

    def __init__( self, bucket_name ):

        try:

            print( f'-> initializing GCS client using bucket {bucket_name}' )

            self._client = storage.Client()
            self._bucket = self._client.bucket( bucket_name )

        except DefaultCredentialsError:

            print( f"xx> missing GCP credentials file, add it & set GOOGLE_APPLICATION_CREDENTIALS in .env" )
            exit()


    # gcs_path = relative from bucket name, so e.g. diseases/trachoma/data/...
    def get_blob( self, gcs_path ):
        print( f'-> fetching {gcs_path} from cloud storage' )
        blob = self._bucket.blob( gcs_path )
        try:
            bytes = blob.download_as_bytes()
            return bytes
        except exceptions.NotFound as h:
            print( f"xx> file not found in GCS: {gcs_path}" )
            sys.exit( 1 )

    def blob_exists( self, gcs_path ):
        blob = self._bucket.blob( gcs_path )
        found = blob.exists()
        return found

    def write_string_to_file( self, string, gcs_path ):
        blob = self._bucket.blob( gcs_path )
        blob.upload_from_string( string )

    def download_blob_to_file( self, gcs_path, file_path ):
        blob = self._bucket.blob( gcs_path )
        blob.download_to_filename( file_path )

    def upload_file_to_blob( self, file_path, gcs_path ):
        blob = self._bucket.blob( gcs_path )
        blob.upload_from_filename( file_path )
