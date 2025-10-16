import abc

class DocumentService(abc.ABC):
    def __init__(self):
        pass

    @abc.abstractmethod
    def new_document(self):
        """
        Create a new document based on information in this class.
        """
        pass

    @abc.abstractmethod
    def search_query(self):
        """
        Create a search query to find this object as a document.
        """
        pass