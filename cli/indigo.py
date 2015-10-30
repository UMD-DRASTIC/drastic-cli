"""Indigo Command Line Interface.

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

__doc_opt__ = """
Indigo Command Line Interface.

Usage:
  indigo init [--url=<URL>] [--username=<USER>] [--password=<PWD>]
  indigo exit
  indigo pwd
  indigo ls [<path>]
  indigo cd [<path>]
  indigo mkdir <path>
  indigo put <src> [<dest>] [--mimetype=<MIME>]
  indigo get <src> [<dest>] [--force]
  indigo rm <path> [--recursive]
  indigo meta add <path> <meta_name> <meta_value>
  indigo meta set <path> <meta_name> <meta_value>
  indigo meta rm <path> <meta_name> [<meta_value>]
  indigo meta ls <path> [<meta_name>]
  
  indigo (-h | --help)
  indigo --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --url=<URL>   Location of Indigo server [default: http://127.0.0.1]

"""


from docopt import docopt
from blessings import Terminal
import os
import pickle
import urllib2
from getpass import getpass
from operator import methodcaller
import errno
from collections import defaultdict

import cli
from cli.client import IndigoClient
from cli.errors import (
    HTTPError,
    IndigoClientError,
    IndigoConnectionError,
    NoSuchObjectError,
    ObjectConflictError
)

SESSION_PATH = os.path.join(os.path.expanduser('~/.indigo'),
                            'session.pickle'
                            )

class IndigoApplication(object):

    def __init__(self, session_path):
        self.terminal = Terminal()
        self.session_path = session_path


    def cd(self, args):
        "Move into a different container."
        client = self.get_client(args)
        path = args['<path>']
        try:
            client.chdir(path)
        except NoSuchObjectError as e:
            print ("cd: {0}: No such object or container"
                   "".format(path))
            return e.errno
        # Save the client for future use
        self.save_client(client)


    def create_client(self, args):
        """Return a IndigoClient."""
        url = args['--url']
        client = IndigoClient(url)
        # Test for client connection errors here
        try:
            json = client.get_cdmi('/')
        except HTTPError as e:
            if e.code in [401, 403]:
                # Allow for authentication to take place later
                return client
            raise e
        return client

    def exit(self, args):
        "Close CDMI client session"
        try:
            os.remove(self.session_path)
        except OSError:
            # No saved client to log out
            pass


    def get(self, args):
        "Fetch a data object from the archive to a local file."
        # Determine local filename
        if args['<dest>']:
            localpath = args['<dest>']
        else:
            localpath = args['<src>'].rsplit('/')[-1]
    
        # Check for overwrite of existing file, directory, link
        if os.path.isfile(localpath):
            if not args['--force']:
                print ("get: {0}: "
                       "File exists, --force option not used"
                       "".format(localpath)
                       )
                return errno.EEXIST
        elif os.path.isdir(localpath):
            print ("get: {0}: "
                   "Is a directory"
                   "".format(localpath))
            return errno.EISDIR
        elif os.path.exists(localpath):
            print ("get: {0}: "
                   "Exists but not a file"
                   "".format(localpath))
            return errno.EEXIST
    
        client = self.get_client(args)
        try:
            with client.open(args['<src>']) as cfh, open(localpath, 'wb') as lfh:
                lfh.write(cfh.text)
        except NoSuchObjectError as e:
            print ("get: {0}: No such object or container"
                   "".format(args.path))
            return e.code
        else:
            print(localpath)


    def get_client(self, args):
        """Return a IndigoClient.
    
        This may be achieved by loading a IndigoClient with a previously saved
        session.
        """
        url = args['--url']
        try:
            # Load existing session, so as to keep current dir etc.
            with open(self.session_path, 'rb') as fh:
                client = pickle.load(fh)
        except (IOError, pickle.PickleError):
            # Init a new IndigoClient
            client = self.create_client(args)
        if client.url != url:
            # Init a fresh IndigoClient
            client = self.create_client(args)
        return client


    def init(self, args):
        """Initialize a CDMI client session.
    
        Optionally log in using HTTP Basic username and password credentials.
        """
        client = self.get_client(args)
        if args['--username']:
            if not args['--password']:
                # Request password from interactive prompt
                args['--password'] = getpass("Password: ")
    
            success = client.authenticate(args['--username'], args['--password'])
            if success:
                print('Successfully logged in as {0.bold}{1}{0.normal}'
                      ''.format(self.terminal, args['--username'])
                      )
            else:
                # Failed to log in
                # Exit without saving client
                print('{0.bold_red}Failed{0.normal} - Login credentials not '
                      'recognized'.format(self.terminal)
                      )
                return 401
        print self.terminal.green("Connected")
        # Save the client for future use
        self.save_client(client)
        return 0


    def ls(self, args):
        """List a container or object."""
        client = self.get_client(args)
        path = args['<path>']
        try:
            cdmi_info = client.ls(path)
        except (NoSuchObjectError) as e:
            print ("ls: cannot access {0}: No such container"
                   "".format(path))
            return 404
        if cdmi_info[u'objectType'] == u'application/cdmi-container':
            containers = [x for x in cdmi_info[u'children'] if x.endswith('/')]
            objects = [x for x in cdmi_info[u'children'] if not x.endswith('/')]
            for child in sorted(containers, key=methodcaller('lower')):
                print self.terminal.blue(child)
            for child in sorted(objects, key=methodcaller('lower')):
                print child
        else:
            print cdmi_info[u'objectName']
        return 0


    def meta_add(self, args, replace=False):
        """Add metadata"""
        client = self.get_client(args)
        path = args['<path>']
        if path == '.' or path == './':
            path = client.pwd()
        cdmi_info = client.get_cdmi(path)
        metadata = cdmi_info['metadata']
        
        attr = args['<meta_name>']
        val = args['<meta_value>']

        if metadata.has_key(attr):
            if replace:
                metadata[attr] = val
            else:
                try:
                    # Already a list, we add it
                    metadata[attr].append(val)
                except AttributeError:
                    # Only 1 element, we create a list
                    metadata[attr] = [metadata[attr], val]
        else:
            metadata[attr] = val

        cdmi_info = client.put(path, metadata=metadata)


    def meta_ls(self, args):
        """List metadata"""
        client = self.get_client(args)
        path = args['<path>']
        if path == '.' or path == './':
            path = client.pwd()
        cdmi_info = client.get_cdmi(path)
        
        if args['<meta_name>']:
            # List 1 field
            if cdmi_info['metadata'].has_key(args['<meta_name>']):
                print('{0}:{1}'.format(args['<meta_name>'],
                                       cdmi_info['metadata'][args['<meta_name>']]))
        else:
            # List everything
            for attr, val in cdmi_info['metadata'].iteritems():
                if attr.startswith(('cdmi_', 'com.archiveanalytics.indigo_')):
                    # Ignore non-user defined metadata
                    continue
                if isinstance(val, list):
                    for v in val:
                        print('{0}:{1}'.format(attr, v))
                else:
                    print('{0}:{1}'.format(attr, val))


    def meta_rm(self, args):
        """Remove metadata"""
        client = self.get_client(args)
        path = args['<path>']
        if path == '.' or path == './':
            path = client.pwd()
        cdmi_info = client.get_cdmi(path)
        metadata = cdmi_info['metadata']
        
        attr = args['<meta_name>']
        val = args['<meta_value>']

        if val:
            # Remove a specific value
             ex_val = metadata[attr]
             if isinstance(ex_val, list):
                 # Remove all elements of teh list with value val
                 metadata[attr] = [x for x in ex_val if x != val]
             elif ex_val == val:
                 # Remove a single element if that's the one we wanted to remove
                 del metadata[attr]
        else:
            try:
                del metadata[attr]
            except KeyError:
                # Metadata not defined
                pass

        cdmi_info = client.put(path, metadata=metadata)


    def mkdir(self, args):
        "Create a new container."
        client = self.get_client(args)
        path = args['<path>']
        if not path.startswith("/"):
            # relative path
            path = "{}{}".format(client.pwd(), path)
        try:
            client.mkdir(path)
        except NoSuchResourceError as e:
            print ("mkdir: cannot create container '{0}': "
                   "No such object or container"
                   "".format(path))
            return e.errno
        except (NoSuchCollectionError, ResourceConflictError) as e:
            print ("mkdir: cannot create container '{0}': "
                   "Not a container"
                   "".format(path))
            return e.errno
        except CollectionConflictError as e:
            print ("mkdir: cannot create container '{0}': "
                   "Container exists"
                   "".format(path))
            return e.errno


    def put(self, args):
        "Put a file to a path."
        # Absolutize local path
        localpath = os.path.abspath(args['<src>'])
        try:
            with open(localpath, 'rb') as fh:
                client = self.get_client(args)
                if args['<dest>']:
                    path = args['<dest>']
                else:
                    # PUT to same name in pwd on server
                    path = os.path.basename(localpath)
    
                try:
                    # To avoid reading large files into memory, client.put()
                    # accepts file-like objects
                    cdmi_info = client.put(path, fh, mimetype=args["--mimetype"])
                except NoSuchResourceError as e:
                    print ("put: cannot put data '{0}': "
                           "No such object or container"
                           "".format(path))
                    return e.errno
                print(cdmi_info[u'parentURI'] + cdmi_info[u'objectName'])
    
        except IOError as e:
            print ("put: local file {0}: "
                   "No such file or directory"
                   "".format(args['<src>'])
                   )
            return e.errno


    def pwd(self, args):
        """Print working directory"""
        client = self.get_client(args)
        print client.pwd()


    def rm(self, args):
        "Remove a data object."
        # Check for container without recursive
        path = args['<path>']
        if path.endswith('/') and not args['--recursive']:
            print ("rm: cannot remove '{0}': "
                   "Is a container"
                   "".format(path))
            return errno.EISDIR
    
        client = self.get_client(args)
        try:
            client.delete(path)
        except NoSuchObjectException:
            # Possibly a container given without the trailing
            # Try fetching in order to give correct response
            try:
                cdmi_info = client.get_cdmi(path + "/")
            except NoSuchObjectException as e:
                # It really does not exist!
                print ("rm: cannot remove '{0}': "
                       "No such object or container"
                       "".format(path)
                       )
                return e.errno
    
            # Fixup path and recursively call this function (_rm)
            arg['<path>'] = cdmi_info['parentURI'] + cdmi_info['objectName']
            return _rm(args)

    def save_client(self, client):
        """Save the status of the IndigoClient for subsequent use."""
        if not os.path.exists(os.path.dirname(self.session_path)):
            os.makedirs(os.path.dirname(self.session_path))
        # Load existing session, so as to keep current dir etc.
        with open(self.session_path, 'wb') as fh:
            pickle.dump(client, fh, pickle.HIGHEST_PROTOCOL)


def main():
    arguments = docopt(__doc_opt__,
                       version='Indigo CLI {}'.format(cli.__version__))
    app = IndigoApplication(SESSION_PATH)

    if arguments['init']:
        return app.init(arguments)
    elif arguments['meta']:
        if arguments['add']:
            return app.meta_add(arguments)
        elif arguments['set']:
            return app.meta_add(arguments, True)
        elif arguments['ls']:
            return app.meta_ls(arguments)
        elif arguments['rm']:
            return app.meta_rm(arguments)
    elif arguments['exit']:
        return app.exit(arguments)
    elif arguments['pwd']:
        return app.pwd(arguments)
    elif arguments['ls']:
        return app.ls(arguments)
    elif arguments['cd']:
        return app.cd(arguments)
    elif arguments['mkdir']:
        return app.mkdir(arguments)
    elif arguments['put']:
        return app.put(arguments)
    elif arguments['get']:
        return app.get(arguments)
    elif arguments['rm']:
        return app.rm(arguments)



if __name__ == '__main__':
    main()
