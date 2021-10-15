import boto3
from dss_utils.s3_utilities import S3Interface
from dss_utils.filesystem_utilities import FSInterface


def create_interface(folder_info):
    """Init interface from dataiku folder info

        :param folder_info: dataikuFolder.get_info()['accessInfo']
        :return: Interface
    """
    if folder_info['type'] == 'S3':
        return S3Interface.build_from_folder_info(folder_info)
    elif folder_info['type'] == 'Filesystem':
        return FSInterface.build_from_folder_info(folder_info)
    else:
        raise Exception("Trying to create interface of unknown type")    
