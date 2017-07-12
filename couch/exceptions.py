class CouchError(Exception):
    pass


class RevisionMismatch(CouchError):
    pass


class ObjectDoesNotExist(CouchError):
    pass


class MultipleObjectsReturned(CouchError):
    pass


