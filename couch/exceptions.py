class CouchError(Exception):
    pass


class RevisionMismatch(CouchError):
    pass
