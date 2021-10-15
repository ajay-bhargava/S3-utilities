import boto3

class S3FileStream:
    def __init__(self, s3_client, bucket, folder_prefix, file_name):
        self.bucket = bucket
        self.file_key = folder_prefix + file_name # File to upload (from dataiku dataset)
        self.s3_client = s3_client
        
    def __enter__(self):
        """Open a file from folder as stream

        :return: Output stream
        """
        s3_response_object = self.s3_client.get_object(Bucket=self.bucket, Key=self.file_key)
        return s3_response_object['Body']
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class S3Interface:
    SERVER_SIDE_Encryption = 'AES256'
    s3_client = boto3.client('s3')

    def __init__(self, bucket, folder_prefix):
        self.bucket = bucket # Bucket name. If None the one from initial accessInfo is used
        self.folder_prefix = folder_prefix # Relative path in the bucket. If none then one from initial accessInfo is used
        
    def download_file(self, file_name, local_name):
        file_key = self.folder_prefix + file_name
        self.s3_client.download_file(self.bucket, file_key, local_name)
    
    def copy(self, file_name, other_interface):
        from_file_key = self.folder_prefix + file_name
        copy_source = {
            'Bucket': self.bucket,
            'Key': from_file_key
        }
        to_file_key = other_interface.folder_prefix + file_name
        self.s3_client.copy(copy_source, other_interface.bucket, to_file_key, ExtraArgs={'ServerSideEncryption': self.SERVER_SIDE_Encryption})
        
    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket

        :param fileName: File to upload
        :param object_name: S3 object name. If not specified then fileName is used
        """
        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = self.folder_prefix + "/" + file_name
        else:
            object_name = self.folder_prefix + "/" + object_name
        self.s3_client.upload_file(file_name, self.bucket, object_name, ExtraArgs={'ServerSideEncryption': self.SERVER_SIDE_Encryption})


    def upload_data(self, data, file_name):
        """Upload data to an S3 bucket

        :param data: Data to upload
        :param fileName File key (s3)
        """

        file_key = self.folder_prefix + file_name
        self.s3_client.put_object(Body=data, Bucket=self.bucket, Key=file_key, ServerSideEncryption=self.SERVER_SIDE_Encryption)

                
    def open(self, file_name):
        return S3FileStream(self.s3_client, self.bucket, self.folder_prefix, file_name)
    
    @staticmethod
    def build_from_folder_info(folder_info):
        access_info = folder_info['accessInfo']
        folder_prefix = access_info['root'][1:]
        bucket =  access_info['bucket']
        return S3Interface(bucket, folder_prefix)
    

