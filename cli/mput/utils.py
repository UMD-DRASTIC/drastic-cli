"""

Drastic Command Line Interface -- multiple put.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


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
        :param client: DrasticClient
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
