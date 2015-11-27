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

from .config import NUM_THREADS
from .mput import mput
from .mput_execute import mput_execute
from .mput_prepare import mput_prepare
from .mput_status import mput_status

__all__ = ('mput', 'mput-prepare', 'mput_status', 'mput_execute', 'NUM_THREADS', '_dirmgmt')



#
