from pymongo import MongoClient
from gridfs import GridFSBucket, GridFS
from gridfs.errors import NoFile
from gridfs.grid_file import GridIn, GridOut
from ..settings import get_db_connection_string
from pymongo.errors import ServerSelectionTimeoutError
from bson.objectid import ObjectId
from pymongo.collection import Collection
from .. import DIDDocument


class BinaryDoc:
    def __init__(self, grid_file):
        if not isinstance(grid_file, GridIn) or not isinstance(grid_file, GridOut):
            raise ValueError(
                "grid_file needs to be an instance of grid_file.GridIn or grid_file.GridOut")
        self.__gridfs__ = grid_file

    @property
    def grid_file(self):
        return self.__gridfs__

    def fseek(self, pos):
        if not isinstance(self.__gridfs__, GridOut):
            raise ValueError("This binary doc not open for reading")
        self.__gridfs__.seek(pos)

    def ftell(self):
        if not isinstance(self.__gridfs__, GridOut):
            raise ValueError("This binary file is not open for reading")
        return self.__gridfs__.tell

    def fwrite(self, data):
        if not isinstance(self.__gridfs__, GridIn):
            raise ValueError("This binary file is not open for writing")
        self.__gridfs__.write(data)

    def fread(self, size=-1):
        if not isinstance(self.__gridfs__, GridOut):
            raise ValueError("This binary file is not open for reading")
        return self.__gridfs__.read(size)

    def fclose(self):
        if self.__gridfs__.closed:
            raise RuntimeError("The binary doc has already closed")
        else:
            self.__gridfs__.close()


class GridFSBinary:
    def __init__(
            self,
            gridfs_connection=None,
            bucket_name='fs',
            metadata='metadata'):
        def __make_connection(connection_string):
            try:
                client = MongoClient(connection_string)
                client.server_info()
                return client
            except ServerSelectionTimeoutError:
                raise ConnectionError(
                    "Fail the connect to the database @{}".format(connection_string))

        if gridfs_connection is None:
            gridfs_connection = get_db_connection_string('gridfs')
        self.conn = __make_connection(gridfs_connection)
        self.db = self.conn['did_gridfs_binary']
        self.fs = GridFS(database=self.db, collection=bucket_name)
        self.fs_bucket = GridFSBucket(db=self.db, bucket_name=bucket_name)
        self.metadata = self.db[metadata]

    def upload_new_file(self, file_name, data, did_document):
        self._check_file_existence(did_document, file_name, exist=False)
        file_id = self.fs_bucket.upload_from_stream(file_name, data)
        new_binary_doc = {'document_id': did_document.id,
                          'filename': file_name,
                          'id': file_id}
        self.metadata.insert_one(new_binary_doc)

    def download_file(self, file_name, did_document, destination):
        file_destination = open(destination, 'wb+')
        files_info = self._check_file_existence(did_document, file_name)
        self.fs_bucket.download_to_stream(file_id=files_info[file_name], destination=file_destination)
        file_destination.close()
    
    def _check_file_existence(self, did_document, file_name, exist=True):
        files_info = self.list_files(did_document)
        if exist:
            if file_name in files_info:
                raise FileNotFoundError(
                    "File name: {} is not associated with this document, \
                            here are the list of associated binary file: {}".format(
                        file_name, files_info)
                )
        else:
            if file_name in files_info:
                raise ValueError(
                    "File name: {} is already associated with this document, \
                            here are the list of associated binary file: {}".format(
                        file_name, files_info)
                )
        return files_info

    def open_write_stream(self, did_document, file_name, save=None):
        self._check_file_existence(did_document, file_name, exist=True)
        new_binary_doc = {'document_id': did_document.id,
                            'filename': file_name,
                            'id': ObjectId()}
        self.metadata.insert_one(new_binary_doc)
        grid_file = self.fs_bucket.open_upload_stream(file_name)
        return BinaryDoc(grid_file)

    def open_read_stream(self, did_document, file_name):
        files_info = self._check_file_existence(did_document, file_name, exist=True)
        try:
            gridout = self.fs_bucket.open_download_stream(
                file_id=files_info[file_name])
        except NoFile:
            raise FileNotFoundError(
                "Metadata is not in sync with the binary files")
        return BinaryDoc(gridout)

    def remove_file(self, did_document, file_name):
        files_info = self._check_file_existence(did_document, file_name, exist=True)
        try:
            self.fs_bucket.delete(file_id=files_info[file_name])
        except NoFile:
            raise FileNotFoundError(
                "Metadata is not in sync with the binary files")
        self.metadata.delete_one({'id': files_info[file_name]})

    def list_files(self, did_document):
        info_for_doc = self.metadata.find({'document_id': did_document.id})
        files_info = {}
        for info in info_for_doc:
            files_info[info['filename']] = info['id']
        return files_info
