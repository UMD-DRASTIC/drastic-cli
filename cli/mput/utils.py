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

import os
from collections import OrderedDict

### Pull paths from the database and put 'em ...
class LimitedSizeDict(OrderedDict):
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size_limit", 4096)
        OrderedDict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def set(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.popitem(last=False)


class _dirmgmt(LimitedSizeDict):

    def __init__(self, *args, **kwds):
        LimitedSizeDict.__init__(self, *args, **kwds)

    def getdir(self, path, client):
        """

        :param path: basestring
        :param client: IndigoClient
        :return:
        """
        if path in self: return True
        rq = client.get_cdmi(path + '/')
        if not rq.ok():
            p1, n1 = os.path.split(path)
            if p1 not in self:
                self.getdir(p1, client)
            client.mkdir(path+'/')
            self.set(path, True)
        return True
