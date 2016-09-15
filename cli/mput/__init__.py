"""

Drastic Command Line Interface -- multiple put.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from .config import NUM_THREADS
from .mput import mput
from .mput_execute import mput_execute
from .mput_prepare import mput_prepare
from .mput_status import mput_status

__all__ = ('mput', 'mput-prepare', 'mput_status', 'mput_execute', 'NUM_THREADS', '_dirmgmt')



#
