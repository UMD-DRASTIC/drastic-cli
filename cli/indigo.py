#!/usr/bin/python
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
  indigo init --url=<URL> [--username=<USER>] [--password=<PWD>]
  indigo whoami
  indigo exit
  indigo pwd
  indigo ls [<path>] [-a]
  indigo cd [<path>]
  indigo cdmi <path>
  indigo mkdir <path>
  indigo put <src> [<dest>] [--mimetype=<MIME>]
  indigo put --ref <url> <dest> [--mimetype=<MIME>]
  indigo get <src> [<dest>] [--force]
  indigo rm <path>
  indigo chmod <path> (read|write|null) <group>
  indigo meta add <path> <meta_name> <meta_value>
  indigo meta set <path> <meta_name> <meta_value>
  indigo meta rm <path> <meta_name> [<meta_value>]
  indigo meta ls <path> [<meta_name>]
  indigo admin lu [<name>]
  indigo admin lg [<name>]
  indigo admin mkuser [<name>]
  indigo admin moduser <name> (email | administrator | active | password) [<value>]
  indigo admin rmuser [<name>]
  indigo admin mkgroup [<name>]
  indigo admin rmgroup [<name>]
  indigo admin atg <name> <user> ...
  indigo admin rtg <name> <user> ...
  indigo (-h | --help)
  indigo --version
  indigo mput-prepare [-l label] (--walk <file-list> | --read (<source-dir>|-))
  indigo mput-execute [-D <debug_level>] [-l label] <tgt-dir-in-repo>
  indigo mput --walk <source-dir>     <tgt-dir-in-repo>
  indigo mput --read (<file-list>|-)  <tgt-dir-in-repo>
  indigo mput-status [-l label] [--reset] [(--clear|--clean)]

Options:
  -h --help     Show this screen.
  --version     Show version.
  --url=<URL>   Location of Indigo server
  -l --label    a label to have multiple prepares and executes simultaneously [  default: transfer ]
  --reset       reset all 'in-progress' entries to 'ready' in the work queue
  --clear       remove all the entries in the workqueue
  --clean       remove all the 'DONE' entries in the workqueue
  -D <debug_level>  trace/debug statements, integer >= 0  [ default: 0 ]


Arguments:
  <tgt-dir-in-repo>    where to place the files when you inject them [ default: / ]



"""

import errno
import os
import pickle
import sys
from getpass import getpass
from operator import methodcaller
import json

import requests
from requests.exceptions import ConnectionError
from blessings import Terminal
from docopt import docopt

import cli
from cli.acl import (
    cdmi_str_to_str_acemask,
    str_to_cdmi_str_acemask
)
from cli.client import IndigoClient

SESSION_PATH = os.path.join(os.path.expanduser('~/.indigo'),  'session.pickle'   )


class IndigoApplication(object):
    """Methods for the CLI"""

    def __init__(self, session_path):
        self.terminal = Terminal()
        self.session_path = session_path

    def admin_atg(self, args):
        """Add user(s) to a group."""
        client = self.get_client(args)
        groupname = unicode(args['<name>'], "utf-8")
        ls_user = args['<user>']
        res = client.add_user_group(groupname, ls_user)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_lg(self, args):
        """List all groups or a specific group if the name is specified"""
        client = self.get_client(args)
        if args['<name>']:
            name = unicode(args['<name>'], "utf-8")
        else:
            name = None
        if name:
            res = client.list_group(name)
            if not res.ok():
                self.print_error(res.msg())
                return res.code()
            group_info = res.json()
            members = ", ".join(group_info.get("members", []))
            print u"{0.bold}Group name{0.normal}: {1}".format(
                self.terminal,
                group_info.get("name", name))
            print u"{0.bold}Group id{0.normal}: {1}".format(
                self.terminal,
                group_info.get("uuid", ""))
            print u"{0.bold}Members{0.normal}: {1}".format(
                self.terminal,
                members)
        else:
            res = client.list_groups()
            if not res.ok():
                self.print_error(res.msg())
                return res.code()
            for groupname in res.msg():
                print groupname

    def admin_lu(self, args):
        """List all users or a specific user if the name is specified"""
        client = self.get_client(args)
        if args['<name>']:
            name = unicode(args['<name>'], "utf-8")
        else:
            name = None
        if name:
            res = client.list_user(name)
            if not res.ok():
                self.print_error(res.msg())
                return res.code()
            user_info = res.json()
            groups = u", ".join([el['name']
                                 for el in user_info.get("groups", [])])
            print u"{0.bold}User name{0.normal}: {1}".format(
                self.terminal,
                user_info.get("username", name))
            print u"{0.bold}Email{0.normal}: {1}".format(
                self.terminal,
                user_info.get("email", ""))
            print u"{0.bold}User id{0.normal}: {1}".format(
                self.terminal,
                user_info.get("uuid", ""))
            print u"{0.bold}Administrator{0.normal}: {1}".format(
                self.terminal,
                user_info.get("administrator", False))
            print u"{0.bold}Active{0.normal}: {1}".format(
                self.terminal,
                user_info.get("active", False))
            print u"{0.bold}Groups{0.normal}: {1}".format(
                self.terminal,
                groups)
        else:
            res = client.list_users()
            if not res.ok():
                self.print_error(res.msg())
                return res.code()
            for username in res.msg():
                print username

    def admin_mkgroup(self, args):
        """Create a new group. Ask in the terminal for mandatory fields"""
        client = self.get_client(args)
        if not args['<name>']:
            groupname = raw_input("Please enter the group name: ")
        else:
            groupname = args['<name>']
        groupname = unicode(groupname, "utf-8")
        res = client.list_group(groupname)
        if res.ok():
            self.print_error(u"Groupname {} already exists".format(groupname))
            return 409          # Conflict
        res = client.create_group(groupname)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_mkuser(self, args):
        """Create a new user. Ask in the terminal for mandatory fields"""
        client = self.get_client(args)
        if not args['<name>']:
            username = raw_input("Please enter the user's username: ")
        else:
            username = args['<name>']
        username = unicode(username, "utf-8")
        res = client.list_user(username)
        if res.ok():
            self.print_error(u"Username {} already exists".format(username))
            return 409          # Conflict
        admin = raw_input("Is this an administrator? [y/N] ")
        email = ""
        while not email:
            email = raw_input("Please enter the user's email address: ")
        password = ""
        while not password:
            password = getpass("Please enter the user's password: ")
        res = client.create_user(username,
                                 email,
                                 admin.lower() == 'y',
                                 password)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_moduser(self, args):
        """Moduser a new user. Ask in the terminal if the value isn't
        provided"""
        client = self.get_client(args)
        value = unicode(args['<value>'], "utf-8")
        name = unicode(args['<name>'], "utf-8")
        if not value:
            if args['password']:
                while not value:
                    value = getpass("Please enter the new password: ")
            else:
                while not value:
                    value = raw_input("Please enter the new value: ")
                value = unicode(args['<value>'], "utf-8")
        d = {}
        if args['email']:
            d['email'] = value
        elif args['administrator']:
            d['administrator'] = value.lower() in ["true", "y", "yes"]
        elif args['active']:
            d['active'] = value.lower() in ["true", "y", "yes"]
        elif args['password']:
            d['password'] = value
        res = client.mod_user(name, d)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_rmgroup(self, args):
        """Remove a group."""
        client = self.get_client(args)
        if not args['<name>']:
            groupname = raw_input("Please enter the group name: ")
        else:
            groupname = args['<name>']
        groupname = unicode(args['<name>'], "utf-8")
        res = client.rm_group(groupname)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_rmuser(self, args):
        """Remove a user."""
        client = self.get_client(args)
        if not args['<name>']:
            username = raw_input("Please enter the user's username: ")
        else:
            username = args['<name>']
        username = unicode(username, "utf-8")
        res = client.rm_user(username)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def admin_rtg(self, args):
        """Remove user(s) to a group."""
        client = self.get_client(args)
        groupname = args['<name>']
        groupname = unicode(args['<name>'], "utf-8")
        ls_user = args['<user>']
        res = client.rm_user_group(groupname, ls_user)
        if res.ok():
            self.print_success(res.msg())
        else:
            self.print_error(res.msg())
            return res.code()

    def cd(self, args):
        "Move into a different container."
        client = self.get_client(args)
        if args['<path>']:
            path = unicode(args['<path>'], "utf-8")
        else:
            path = u"/"
        res = client.chdir(path)
        if res.ok():
            # Save the client for future use
            self.save_client(client)
        else:
            self.print_error(res.msg())

    def cdmi(self, args):
        "Display cdmi information (dict) for a path."
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        res = client.get_cdmi(path)
        if res.ok():
            print "{} :".format(client.normalize_cdmi_url(path))
            d = res.json()
            for key, value in d.iteritems():
                if key != "value":
                    print u"  - {0.bold}{1}{0.normal}: {2}".format(
                        self.terminal,
                        key,
                        value)
        else:
            self.print_error(res.msg())

    def chmod(self, args):
        "Add or remove ACE to a path."
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        group = unicode(args['<group>'], "utf-8")
        if args['read']:
            level = "read"
        elif args['write']:
            level = "read/write"
        else:
            level = "null"
        ace = {"acetype" : "ALLOW",
               "identifier" : group,
               "aceflags": "CONTAINER_INHERIT, OBJECT_INHERIT",
               "acemask" : str_to_cdmi_str_acemask(level, False)}
        metadata = {"cdmi_acl" : [ace]}
        res = client.put(path, metadata=metadata)
        if res.ok():
            self.print_success(res.msg())
        else:
            if res.code() == 403:
                self.print_error("You don't have the rights to access ACL for this collection")
            else:
                self.print_error(res.msg())
            return res.code()

    def create_client(self, args):
        """Return a IndigoClient."""
        url = args['--url']
        if not url:
            # Called without being connected
            self.print_error("You need to be connected to access the server.")
            sys.exit(-1)
        client = IndigoClient(url)
        # Test for client connection errors here
        res = client.get_cdmi('/')
        if res.code() in [0, 401, 403]:
            # 0 means success
            # 401/403 means authentication problem, we allow for authentication
            # to take place later
            return client
        else:
            self.print_error(res.msg())
            sys.exit(res.code())

    def exit(self, args):
        "Close CDMI client session"
        try:
            os.remove(self.session_path)
        except OSError:
            # No saved client to log out
            pass

    def get(self, args):
        "Fetch a data object from the archive to a local file."
        src = unicode(args['<src>'], "utf-8")
        # Determine local filename
        if args['<dest>']:
            localpath = unicode(args['<dest>'], "utf-8")
        else:
            localpath = src.rsplit('/')[-1]

        # Check for overwrite of existing file, directory, link
        if os.path.isfile(localpath):
            if not args['--force']:
                self.print_error(
                    u"File '{0}' exists, --force option not used"
                     "".format(localpath))
                return errno.EEXIST
        elif os.path.isdir(localpath):
            self.print_error(u"'{0}' is a directory".format(localpath))
            return errno.EISDIR
        elif os.path.exists(localpath):
            self.print_error(u"'{0}'exists but not a file".format(localpath))
            return errno.EEXIST

        client = self.get_client(args)
        try:
            cfh = client.open(src)
            if cfh.status_code == 404:
                self.print_error(u"'{0}': No such object or container"
                                  "".format(src))
                return 404
        except ConnectionError as e:
            self.print_error("'{0}': Redirection failed - Reference isn't accessible"
                                 "".format(e.request.url, e.strerror))
            return 404
        lfh = open(localpath, 'wb')
        for chunk in cfh.iter_content(8192):
            lfh.write(chunk)
        lfh.close()
        print localpath

    def get_client(self, args):
        """Return a IndigoClient.

        This may be achieved by loading a IndigoClient with a previously saved
        session.
        """
        try:
            # Load existing session, so as to keep current dir etc.
            with open(self.session_path, 'rb') as fh:
                client = pickle.load(fh)
        except (IOError, pickle.PickleError):
            # Init a new IndigoClient
            client = self.create_client(args)
        if args['--url']:
            if client.url != args['--url']:
                # Init a fresh IndigoClient
                client = self.create_client(args)
        client.session = requests.Session()
        return client

    def init(self, args):
        """Initialize a CDMI client session.

        Optionally log in using HTTP Basic username and password credentials.
        """
        client = self.get_client(args)
        if args['--username']:
            username = unicode(args['--username'], "utf-8")
        else:
            username = None
        if args['--password']:
            password = unicode(args['--password'], "utf-8")
        else:
            password = None
        if username:
            if not password:
                # Request password from interactive prompt
                password = getpass("Password: ")

            res = client.authenticate(username, password)
            if res.ok():
                print (u"{0.bold_green}Success{0.normal} - {1} as "
                       "{0.bold}{2}{0.normal}".format(self.terminal,
                                                      res.msg(),
                                                      username))
            else:
                print u"{0.bold_red}Failed{0.normal} - {1}".format(
                    self.terminal,
                    res.msg())
                # Failed to log in
                # Exit without saving client
                return res.code()
        else:
            print ("{0.bold_green}Connected{0.normal} -"
                   " Anonymous access".format(self.terminal))
        # Save the client for future use
        self.save_client(client)
        return 0

    def ls(self, args):
        """List a container."""
        client = self.get_client(args)
        if args['<path>']:
            path = unicode(args['<path>'], "utf-8")
        else:
            path = None
        res = client.ls(path)
        if res.ok():
            cdmi_info = res.json()
            pwd = client.pwd()
            if path == None:
                if pwd == "/":
                    print "Root:"
                else:
                    print u"{}:".format(pwd)
            else:
                print u"{}{}:".format(pwd, path)
            # Display Acl
            if args['-a']:
                metadata = cdmi_info.get("metadata", {})
                cdmi_acl = metadata.get("cdmi_acl", [])
                if cdmi_acl:
                    for ace in cdmi_acl:
                        print "  ACL - {}: {}".format(
                            ace['identifier'],
                            cdmi_str_to_str_acemask(ace['acemask'], False)
                            )
                else:
                    print "  ACL: No ACE defined"
            
            if cdmi_info[u'objectType'] == u'application/cdmi-container':
                containers = [x
                              for x in cdmi_info[u'children']
                              if x.endswith('/')]
                objects = [x
                           for x in cdmi_info[u'children']
                           if not x.endswith('/')]
                for child in sorted(containers, key=methodcaller('lower')):
                    print self.terminal.blue(child)
                for child in sorted(objects, key=methodcaller('lower')):
                    print child
            else:
                print cdmi_info[u'objectName']
            return 0
        else:
            self.print_error(res.msg())

    def meta_add(self, args, replace=False):
        """Add metadata"""
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        meta_name = unicode(args['<meta_name>'], "utf-8")
        meta_value = unicode(args['<meta_value>'], "utf-8")
        if path == '.' or path == './':
            path = client.pwd()
        res = client.get_cdmi(path)
        if not res.ok():
            self.print_error(res.msg())
            return res.code()
        cdmi_info = res.json()
        metadata = cdmi_info['metadata']
        if meta_name in metadata:
            if replace:
                metadata[meta_name] = meta_value
            else:
                try:
                    # Already a list, we add it
                    metadata[meta_name].append(meta_value)
                except AttributeError:
                    # Only 1 element, we create a list
                    metadata[meta_name] = [metadata[meta_name], meta_value]
        else:
            metadata[meta_name] = meta_value
        res = client.put(path, metadata=metadata)
        if not res.ok():
            self.print_error(res.msg())
            return res.code()

    def meta_ls(self, args):
        """List metadata"""
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        if args['<meta_name>']:
            meta_name = unicode(args['<meta_name>'], "utf-8")
        else:
            meta_name = None
        if path == '.' or path == './':
            path = client.pwd()
        res = client.get_cdmi(path)
        if not res.ok():
            self.print_error(res.msg())
            return res.code()
        cdmi_info = res.json()
        if meta_name:
            # List 1 field
            if meta_name in cdmi_info['metadata']:
                print(u'{0}:{1}'.format(
                    meta_name,
                    cdmi_info['metadata'][meta_name]))
        else:
            # List everything
            for attr, val in cdmi_info['metadata'].iteritems():
                if attr.startswith(('cdmi_',
                                    'com.archiveanalytics.indigo_')):
                    # Ignore non-user defined metadata
                    continue
                if isinstance(val, list):
                    for v in val:
                        print u'{0}:{1}'.format(attr, v)
                else:
                    print u'{0}:{1}'.format(attr, val)

    def meta_rm(self, args):
        """Remove metadata"""
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        meta_name = unicode(args['<meta_name>'], "utf-8")
        if args['<meta_value>']:
            meta_value = unicode(args['<meta_value>'], "utf-8")
        else:
            meta_value = None
        if path == '.' or path == './':
            path = client.pwd()
        res = client.get_cdmi(path)
        if not res.ok():
            self.print_error(res.msg())
            return res.code()
        cdmi_info = res.json()
        metadata = cdmi_info['metadata']
        if meta_value:
            # Remove a specific value
            ex_val = metadata.get(meta_name, None)
            if isinstance(ex_val, list):
                # Remove all elements of teh list with value val
                metadata[meta_name] = [x for x in ex_val if x != meta_value]
            elif ex_val == meta_value:
                # Remove a single element if that's the one we wanted to
                # remove
                del metadata[meta_name]
        else:
            try:
                del metadata[meta_name]
            except KeyError:
                # Metadata not defined
                pass
        res = client.put(path, metadata=metadata)
        if not res.ok():
            self.print_error(res.msg())
            return res.code()

    def mkdir(self, args):
        "Create a new container."
        client = self.get_client(args)
        path = unicode(args['<path>'], "utf-8")
        if not path.startswith("/"):
            # relative path
            path = u"{}{}".format(client.pwd(), path)
        res = client.mkdir(path)
        if not res.ok():
            self.print_error(res.msg())

    def mput(self, arguments):
        import mput
        return mput.mput(self,arguments)

    def mput_execute(self,arguments):
        import mput
        return mput.mput_execute(self, arguments)

    def mput_prepare(self,arguments):
        import mput
        return mput.mput_prepare(self, arguments)

    def mput_status(self,arguments):
        import mput
        return mput.mput_status(self, arguments)

    def print_error(self, msg):
        """Display an error message."""
        print u"{0.bold_red}Error{0.normal} - {1}".format(self.terminal,
                                                          msg)

    def print_success(self, msg):
        """Display a success message."""
        print u"{0.bold_green}Success{0.normal} - {1}".format(self.terminal,
                                                              msg)

    def print_warning(self, msg):
        """Display a warning message."""
        print u"{0.bold_blue}Warning{0.normal} - {1}".format(self.terminal, msg)

    def put(self, args):
        "Put a file to a path."
        if args["--ref"]:
            return self.put_reference(args)
        src = unicode(args['<src>'], "utf-8")
        # Absolutize local path
        local_path = os.path.abspath(src)
        if args['<dest>']:
            dest = unicode(args['<dest>'], "utf-8")
        else:
            # PUT to same name in pwd on server
            dest = os.path.basename(local_path)
        if not os.path.exists(local_path):
            self.print_error("File '{}' doesn't exist".format(local_path))
            return errno.ENOENT
        with open(local_path, 'rb') as fh:
            client = self.get_client(args)
            # To avoid reading large files into memory,
            # client.put() accepts file-like objects
            res = client.put(dest, fh, mimetype=args["--mimetype"])
            if res.ok():
                cdmi_info = res.json()
                print cdmi_info[u'parentURI'] + cdmi_info[u'objectName']
            else:
                self.print_error(res.msg())

    def put_reference(self, args):
        "Create a reference at path dest with the url."
        dest = unicode(args['<dest>'], "utf-8")
        url = args['<url>']
        client = self.get_client(args)
        dict_data = {"reference": url}
        if args["--mimetype"]:
            dict_data['mimetype'] = args["--mimetype"]
        data = json.dumps(dict_data)
        res = client.put_cdmi(dest, data)
        if res.ok():
            cdmi_info = res.json()
            print cdmi_info[u'parentURI'] + cdmi_info[u'objectName']
        else:
            self.print_error(res.msg())

    def pwd(self, args):
        """Print working directory"""
        client = self.get_client(args)
        print client.pwd()

    def rm(self, args):
        """Remove a data object or a collection.

        If we forget the trailing '/' for a collection we try to add it.
        """
        path = unicode(args['<path>'], "utf-8")
        client = self.get_client(args)
        res = client.delete(path)
        if res.code() == 404:
            # Possibly a container given withouttrailing
            # Try fetching in order to give correct response
            res = client.get_cdmi(path + "/")
            if not res.ok():
                # It really does not exist!
                self.print_error((u"Cannot remove '{0}': "
                                   "No such object or container)"
                                   "".format(path)))
                return 404
            cdmi_info = res.json()
            # Fixup path and recursively call this function (rm)
            args['<path>'] = u"{}{}".format(cdmi_info['parentURI'],
                                            cdmi_info['objectName'])
            return self.rm(args)

    def save_client(self, client):
        """Save the status of the IndigoClient for subsequent use."""
        if not os.path.exists(os.path.dirname(self.session_path)):
            os.makedirs(os.path.dirname(self.session_path))
        # Load existing session, so as to keep current dir etc.
        with open(self.session_path, 'wb') as fh:
            pickle.dump(client, fh, pickle.HIGHEST_PROTOCOL)

    def whoami(self, args):
        """Print name of the user"""
        client = self.get_client(args)
        print client.whoami() + " - " + client.url


def main():
    """Main function"""
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

    elif arguments['admin']:
        if arguments['lu']:
            return app.admin_lu(arguments)
        if arguments['lg']:
            return app.admin_lg(arguments)
        if arguments['mkuser']:
            return app.admin_mkuser(arguments)
        if arguments['moduser']:
            return app.admin_moduser(arguments)
        if arguments['rmuser']:
            return app.admin_rmuser(arguments)
        if arguments['mkgroup']:
            return app.admin_mkgroup(arguments)
        if arguments['rmgroup']:
            return app.admin_rmgroup(arguments)
        if arguments['atg']:
            return app.admin_atg(arguments)
        if arguments['rtg']:
            return app.admin_rtg(arguments)

    elif arguments['chmod']:
        return app.chmod(arguments)
    elif arguments['exit']:
        return app.exit(arguments)
    elif arguments['pwd']:
        return app.pwd(arguments)
    elif arguments['ls']:
        return app.ls(arguments)
    elif arguments['cd']:
        return app.cd(arguments)
    elif arguments['cdmi']:
        return app.cdmi(arguments)
    elif arguments['mkdir']:
        return app.mkdir(arguments)
    elif arguments['put']:
        return app.put(arguments)
    elif arguments['get']:
        return app.get(arguments)
    elif arguments['rm']:
        return app.rm(arguments)
    elif arguments['whoami']:
        return app.whoami(arguments)
    elif arguments['mput-prepare'] :
        return app.mput_prepare(arguments)
    elif arguments['mput-execute'] :
        return app.mput_execute(arguments)
    elif arguments['mput-status'] :
        return app.mput_status(arguments)
    elif arguments['mput'] :
        return app.mput(arguments)


if __name__ == '__main__':
    main()
