from __future__ import annotations
import did.types as T
from did.database.file_system import BinaryCollection
from did.document import DIDDocument

default_options = {
    'auto_save': False,
}

class DID:
    def __init__(self, database, binary_directory, options={}):
        options = { **default_options, **options }

        self.db = database
        self.bin = BinaryCollection(binary_directory, name='data')
        self.auto_save = options.get('auto_save') or False

    def find(self, query, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def add(self, document, save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def update(self, document, save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def upsert(self, document, save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def delete(self, document, save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def find_by_id(self, did_id, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()
    
    def update_by_id(self, did_id, document_updates, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def delete_by_id(self, did_id, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def update_many(self, query, document_updates, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()
    
    def delete_many(self, query, version='', save=False):
        # do stuff
        if self.auto_save or save:
            self.save()

    def save(self):
        self.db.commit()
        # hash new version
        # update version history in database
        # update new version in affected documents? is this doable?
        # self.db.commit() again to save version changes

    def revert(self):
        """Revert database to point of last save"""
        self.db.unstage_changes()

    def open_binary_read(self, document, filename='', version=''):
        pass

    def open_binary_write(self, document, filename='', version=''):
        pass