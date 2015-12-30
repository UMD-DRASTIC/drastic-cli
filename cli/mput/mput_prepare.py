
"""
    DB Wrapping class for the multiple put


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
import os.path
import sys
from .db import DB
from .mput_threads import *


def mput_prepare(app, arguments):
    db = DB(app, arguments)

    ### Instrumentation
    t0 = time.time()
    t1 = t0
    ctr = 0
    ####################
    if arguments['--walk']:
        tree = arguments['<file-list>']
        if '~' in tree : tree = os.path.expanduser(tree)
        tree = os.path.normpath(tree)
        if not tree or not os.path.isdir(tree):
            raise ValueError("can't find the tree to walk <{}>".format(tree))

        for dirname,_,files in os.walk(tree,topdown=True,followlinks=True) :
            for fn in files :
                ctr += 1
                db.insert(os.path.normpath(os.path.join(dirname, fn)).decode('utf-8'))
            t2 = time.time()
            if ( t2 - t1 ) > 30 :
                print '{0:,} registered in {1:.2f} secs -- {2}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))
                t1 = t2
    ####################
    elif arguments['--read'] :
        if arguments['<file-list>'] == '-' : fp = sys.stdin
        else : fp = open(arguments['<file-list>'],'rU')
        for path in fp :
            if not os.path.exists(path) :
                print >>sys.stderr,"skipping -- file does not exist : ",path
                continue
            ctr += 1
            db.insert(os.path.abspath( path).decode('utf-8'))
            if ctr% 5000 :
                t2 = time.time()
                if ( t2 - t1 ) > 30 :
                    print '{0:,} registered in {1:.2f} secs -- {2:.2f}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))
                    t1 = t2

    #####################
    # Summary
    t2 = time.time()
    print '{0:,} registered in {1:.2f} secs -- {2:.2f}/sec'.format(ctr, (t2-t1), ctr / (t2 - t0))

