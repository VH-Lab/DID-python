class DIDDocument:
    def __init__(self, data):
        self.id = data.base['id']
        self.data = data

    @property
    def binary_files(self):
        """ Get binary filenames """
        return self.data.get('binary_files')

    def serialize(self):
        pass