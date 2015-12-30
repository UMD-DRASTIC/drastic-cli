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

import os
import sqlite3
import sys


class DB:
    def __init__(self, app, args):
        p = app.session_path

        # set the label to the first candidate...
        label = filter(bool, [args.get('--label', None), args.get('-l', None), 'transfer'])[0]
        # Create a 'safe' version of the label
        def safe(s):
            import hashlib,base64
            v = base64.b64encode(hashlib.md5(s).digest(),'-#').rstrip('=')
            return v
        if label == 'transfer' :
            safename = 'work_queue-00.db'
        else:
            safename = 'work_queue-{}.db'.format(safe(label))

        # construct the path
        if os.path.isfile(p): p,_ = os.path.split(p)
        self.dbname = os.path.join( p , safename )

        # if the directory doesn't exist try to make it.
        if not os.path.isdir(p):
            try:
                os.mkdir(p)
            except Exception as e:
                print '{}\n -- cannot make directory {} '.format(e,p)
                raise

        # open or create the database
        try :
            self.cnx = sqlite3.connect(self.dbname , check_same_thread = False )
        except Exception as e :
            print e
            raise RuntimeError("Cannot open {}".format(self.dbname))
        #####
        self.cs = self.cnx.cursor()



        self.cs.execute('''CREATE TABLE IF NOT EXISTS transfer
                (row_id INTEGER PRIMARY KEY AUTOINCREMENT ,
                 path TEXT,  name TEXT,
                 state TEXT CHECK (state in ('RDY','WRK','DONE','FAIL')) NOT NULL DEFAULT 'RDY'  ,
                 start_time INTEGER default CURRENT_TIMESTAMP,
                 end_time INTEGER ,
                 UNIQUE ( path,name )
                  ) ''' )

        self.cs.execute('''CREATE INDEX IF NOT EXISTS "t_path1_idx"  ON "transfer"(path)   ''' )
        try:

            self.cs.execute('''CREATE INDEX IF NOT EXISTS "t_state_idx"  ON "transfer"(state) where state =  'DONE' ''' )
            self.cs.execute('''CREATE INDEX IF NOT EXISTS "t_state1_idx" ON "transfer"(state) where state <> 'DONE' ''')
            self.cs.execute('''CREATE INDEX IF NOT EXISTS "t_path_idx"   ON "transfer"(path) WHERE state = 'RDY' ''' )
        except Exception as e :
            print e
            print 'Falling back to full indexes ... you may wish to consider updating your version of sqlite'
            self.cs.connection.rollback()
            self.cs.execute('''CREATE INDEX IF NOT EXISTS t_state_idx  ON transfer (state)''' )     # Fallback to full  index if partial fails.
        self.cs.connection.commit()


    def update(self, rowid, state):
        if state == 'WRK':
            cmd = '''UPDATE transfer SET state = ? , start_time = strftime('%s','now') Where row_id = ?'''
        else:
            cmd = '''UPDATE transfer SET state = ? , end_time = strftime('%s','now') Where row_id = ?'''
        try:
            self.cs.execute(cmd, [state, rowid])
            self.cs.connection.commit()
            return rowid
        except Exception as e:
            print e
            self.cs.connection.rollback()
            return None

    def get_and_lock(self):
        """
            This function will get retrieve an entry where the
        :return:
        """
        self.cs.execute('''BEGIN''')
        ## Select A Path's worth of files.... where some are
        cmd = '''WITH DIR as ( SELECT path from transfer WHERE STATE = 'RDY' order by path LIMIT 1)
                    SELECT path ,name,start_time,end_time,row_id from transfer JOIN DIR USING (path) where STATE = 'RDY'
            '''
        self.cs.execute(cmd)
        results = self.cs.fetchall()

        data = [data[-1:] for data in results]
        cmd = '''UPDATE transfer SET STATE = 'WRK' , start_time = strftime('%s','now') WHERE row_id = ?'''
        if data :
            self.cs.executemany(cmd,data)
            self.cs.connection.commit()
        else :
            self.cs.connection.rollback()
        # And unicode the results...
        return results

    def insert(self, path):
        """
            Put a new path in , or ignore if it is already there.
            :path: Path to put in work queue _if_ not present
        """
        if not os.path.exists(path):
            print >> sys.stderr, '{0} does not exist ...skipping '.format(path)
            return None
        p1, n1 = os.path.split(os.path.normpath(path))  # Avoid naive duplication
        cmd = '''insert or ignore INTO transfer (path,name,state) VALUES ( ? , ? , ? )'''
        self.cs.execute(cmd, (p1, n1, 'RDY'))
        ret =  self.cs.lastrowid
        self.cs.connection.commit()
        return ret

    def status(self, reset=False, clear=False, clean=False):
        friendly = dict(DONE = 'Done',FAIL = 'Failed' , RDY = 'Ready' , WRK = 'Processing')
        self.cs.execute('SELECT state,count(*),avg(end_time-start_time) from transfer group by state order by state' )

        retval = u'{:10s} |{:23s} |{:20s}\n'.format('State', 'Count', 'Average time in State')
        retval += '{:10s} |{:23s} |{:20s}\n'.format('-' * 10, '-' * 23, '-' * 20)

        for state, count, avg in self.cs:

            state = friendly[state]
            ## Odd, avg seems to return a string !?  We shouldn't have to do this!!
            if isinstance(avg, basestring):
                avg = (' ' * 20 + avg)[-20:]  # Make average 20 characters long
            elif isinstance(avg, float):
                avg = '{:20.2f}'.format(avg)  # otherwise format it as 20 chars long
            try:
                retval += '{0:10s} |{1:23,} |{2}\n'.format(state, count, avg)
            except:
                retval += '{0:10s} |{1:23s} |{2}\n'.format(str(state), str(count), str(avg))
            ### Done oddball fix

        ### See if we need to reset the work queue
        cmd = None
        if reset:
            cmd = ("""UPDATE transfer
                        SET state = 'RDY', start_time=strftime('%s','now'), end_time = strftime('%s','now')
                        WHERE STATE = 'FAIL' or 'STATE' = 'WRK'""")
            retval += u'\n\n    Then resetting Failed and Processing values.'
        if clean:
            cmd = '''DELETE from transfer where state = 'DONE' '''
        if clear:
            # Since sqlite doesn't have a truncate command, just drop the table -- it will be recreated if necessary
            cmd = u'drop table transfer'
        #---
        if cmd :
            try:
                self.cs.execute(cmd)
                self.cs.connection.commit()
            except sqlite3.DatabaseError as e:
                print e
                print 'Failed to process cmd -- \n{}\n'.format(cmd)

        # And now return the status
        return retval

