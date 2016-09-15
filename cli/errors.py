"""Drastic Command Line Interface Exceptions.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


class DrasticClientError(Exception):
    """Base Class for Drastic Command Line Interface Exceptions.

    Abstract Base Class from which more specific Exceptions are derived.
    """

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return "Client Error {}: {}".format(self.code, self.msg)


class HTTPError(DrasticClientError):
    """Drastic HTTP Exception."""

    def __str__(self):
        return "HTTP Error {}: {}".format(self.code, self.msg)


class DrasticConnectionError(DrasticClientError):
    """Drastic client connection Exception."""

    def __str__(self):
        return "Connection Error {}: {}".format(self.code, self.msg)


class NoSuchObjectError(DrasticClientError):
    """Drastic client no such object Exception."""

    def __str__(self):
        return "Object already exists at {0}".format(self.msg)


class ObjectConflictError(DrasticClientError):
    """Drastic object already exists Exception."""

    def __str__(self):
        return "Object already exists at {0}".format(self.msg)
