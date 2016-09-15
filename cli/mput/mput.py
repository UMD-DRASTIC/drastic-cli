"""
    DB Wrapping class for the multiple put


    Drastic Command Line Interface -- multiple put.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import os
import sys
import time

from .config import NUM_THREADS
from .mput_threads import thread_setup, file_putter, file_putter_worker
from .utils import _dirmgmt


def mput(app, arguments):
    """
            drastic mput --walk <source-dir>     <tgt-dir-in-repo>
            drastic mput --read (<file-list>|-)  <tgt-dir-in-repo>

    :param "DrasticApplication" app:
    :param arguments:
    :return:
    """

    ####################  Abstract the source of file names into an iterator #########
    if arguments['--walk']:
        src = arguments['<source-dir>']
        if not os.path.isdir(src):
            raise ValueError(src)

        def reader(dirname):
            for path, _, files in os.walk(dirname, topdown=True, followlinks=True):
                for fn in files: yield os.path.abspath(os.path.join(path, fn))

        _src = reader(src)
    elif arguments['--read']:
        if arguments['<file-list>'] == '-':
            fp = sys.stdin
        else:
            fp = open(arguments['<file-list>'], 'rb')

        def reader(fp):
            for l in fp:
                l = l.strip()
                if not l: continue
                yield os.path.normpath(l)

        #### End Function ####
        _src = reader(fp)
    else:
        ### This should never happen !
        raise NotImplementedError('Docopt args inconsistent')
    ####
    #### Now get the list of files and push 'em onto the queue...
    ####
    client = app.get_client(arguments)
    tgtdir = arguments['<tgt-dir-in-repo>']
     ### Set up a directory name cache, so that we don't have to keep going back
    cache = _dirmgmt()

    q, threads = thread_setup(NUM_THREADS, None,   client , cache = cache )

    ### Instrumentation
    t0 = time.time()
    t1 = t0
    ctr = 0
    ### Actual mput loop ###
    for path in _src:
        ctr += 1
        tgtfile = os.path.join(tgtdir, path.strip('/'))
        n1, _ = os.path.split(tgtfile)
        if n1:
            cache.getdir(n1, client)

        if not os.path.isfile(path):
            print >> sys.stderr, "skipping -- file does not exist or is not a dir : ", path
            continue

        print "putting ", (path, tgtfile, None)

        q.put(tuple((path, tgtfile, None)))
        if NUM_THREADS == 0:
            file_putter_worker(q, client,cache)  # forced Serialization for debugging...

    if NUM_THREADS > 0:
        q.join()
    #####################
    # Summary
    t2 = time.time()
    print '{0:,} registered in {1:.2f} secs -- {2:.2f}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))
