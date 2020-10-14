class DIDDocument:
    def __init__(self, data):
        self.id = data.base['id']
        self.data = data

    def serialize(self):
        pass
        # TODO: serialize data to json string