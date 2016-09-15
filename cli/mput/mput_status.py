

"""

Drastic Command Line Interface -- multiple put.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import sys

from .db import DB

def mput_status(app, arguments):
    reset_flag = bool(arguments.get('--reset', False))
    clean_flag = bool(arguments.get('--clean', False))
    clear_flag = bool(arguments.get('--clear', False))
    db = DB(app, arguments)
    print >> sys.stdout, db.status(reset=reset_flag, clear=clear_flag, clean=clean_flag)
    return None
