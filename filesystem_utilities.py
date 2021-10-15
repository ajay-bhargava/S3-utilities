import os
# DataStream-like wrapper for local file system

class FSFileStream:
    def __init__(self, folder_prefix, file_name, mode='r'):
        """Extracts useful information from dataikuFolder info

        :param accessInfo: dataikuFolder.get_info()['accessInfo']. Expected to contain
            root folder (prefix) as ['root']
        """
        self.__file_key = folder_prefix + file_name
        self.mode = mode
        
    def __enter__(self):
        """Open a file from folder as stream

        :param fileName: File to upload (from dataiku dataset)
        :param folderPrefix Relative path in the bucket. If none then one from initial accessInfo is used
        :return: Output stream
        """
        self.__file = open(self.__file_key, self.mode)
        return self.__file
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__file.close()
    
    @staticmethod
    def build_from_access_info(file_name, accessInfo):
        return FSFileStream(accessInfo['root'], file_name)
    
    # TODO: Develop upload stream interface w/o reopening files all the time
    
class FSInterface:
    def __init__(self, folder_prefix):
        self.__folder_prefix = folder_prefix
        
    def open(self, file_name, mode='r'):
        return FSFileStream(self.__folder_prefix, file_name, mode)
    
    @staticmethod
    def build_from_folder_info(folder_info):
        access_info = folder_info['accessInfo']
        folder_prefix = access_info['root']
        return FSInterface(folder_prefix)
