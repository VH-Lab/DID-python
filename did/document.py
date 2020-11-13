import json

class DIDDocument:
    def __init__(self, data):
        try:
            self.id = data.get('base').get('id') 
            self.data = data
        except AttributeError:
            raise Exception('DIDDocuments must be instantiated with a did_document in dict format.')

    @property
    def binary_files(self):
        """ Get binary filenames """
        return self.data.get('binary_files')

    def serialize(self):
        return json.dumps(self.data)