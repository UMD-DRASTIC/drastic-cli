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


class _dirmgmt(set):

    def __init__(self, *args, **kwds):
        from threading import Lock
        super(_dirmgmt,self).__init__(self, *args )
        self.lock =   Lock()

    def  getdir(self,tgtdir, client) :
        """
        :param path: basestring
        :return:
        """
        if tgtdir in self : return True
        dirs = [  tgtdir[:] , ]             # initialize the directory stack
        ### Then walk up the directory stack as far as necessary...
        while dirs :
            res = client.mkdir(dirs[-1])
            if res.ok() :
                tdir = dirs.pop()
                # Update the cache -- safely
                self.lock.acquire()
                self.add(tdir)
                self.lock.release()
                continue

            #### Go here if the mkdir failed... push the parent onto the stack and retry.
            tdir,_ = os.path.split(tdir)
            if tdir != '/' :
                dirs.append(tdir)
            else :
                print "can't make directory {} or some of its parents".format(tgtdir)
                return False

        return True
