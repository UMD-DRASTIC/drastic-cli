

"""

Indigo Command Line Interface -- multiple put.

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import sys

from .db import DB

def mput_status(app, arguments):
    reset_flag = bool(arguments.get('--reset', False))
    clean_flag = bool(arguments.get('--clean', False))
    clear_flag = bool(arguments.get('--clear', False))
    db = DB(app, arguments)
    print >> sys.stdout, db.status(reset=reset_flag, clear=clear_flag, clean=clean_flag)
    return None
