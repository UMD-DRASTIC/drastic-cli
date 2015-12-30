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
import time

### Pull paths from the database and put 'em ...

class counter_timer:
    def __init__(self,label, enabled = True  ):
        self.label = label
        self.enabled = enabled
    def __enter__(self):
        if not self.enabled : return False
        self.T0 = time.time()
    def __exit__(self, exc_type, exc_value, traceback ):
        if not self.enabled : return True
        from sys import stderr
        print >> stderr,"TRACE: {} : elapsed : {:0,.3f}s   ". format(self.label, time.time() - self.T0  )
        return

class _dirmgmt(set):
    def __init__(self, *args ):
        from threading import Lock
        set.__init__(self, *args )
        self.lock =   Lock()

    def  getdir(self,tgtdir, client) :
        """

        :param tgtdir: basestring
        :param client: IndigoClient
        :return:
        """

        if tgtdir in self : return True
        dirs = [  tgtdir[:] , ]             # initialize the directory stack
        ### Then walk up the directory stack as far as necessary...
        while dirs :
            with counter_timer('mkdir_primitive') :
                res = client.mkdir(dirs[-1])
                if res.ok() :
                    tdir = dirs.pop()
                    # Update the cache -- safely
                    self.lock.acquire()
                    self.add(tdir)
                    self.lock.release()
                    continue

            #### Go here if the mkdir failed... push the parent onto the stack and retry.

            tdir,_ = os.path.split( dirs[-1] )
            if tdir != '/' :
                dirs.append(tdir)
            else :
                print "can't make directory {} or some of its parents".format(tgtdir)
                return False

        return True
