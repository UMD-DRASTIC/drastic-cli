"""Indigo CDMI API Client.

Copyright 2014 Archive Analytics Solutions

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


import json
import mimetypes
import mmap
import os
from base64 import b64encode
from urllib import pathname2url, url2pathname

import requests

import cli

CDMI_CONTAINER = 'application/cdmi-container'
CDMI_OBJECT = 'application/cdmi-object'


class Response(object):
    """A Response object returned by the client. It contains an error code and
    a JSON response. 0 or means the code executed correctly.
    """

    def __init__(self, code, msg):
        self._code = code
        if isinstance(msg, dict):
            self._json = msg
        elif isinstance(msg, requests.Response):
            try:
                self._json = msg.json()
            except ValueError:
                self._json = {"msg": msg.content}
        else:
            self._json = {"msg": msg}

    def ok(self):
        """Check if the response is valid or not. Some HTTP error codes like
        201 or 206 can be mapped to 0 to validate a response"""
        return self._code == 0

    def code(self):
        """The code of the response, if it's not a success then it's probably
        an HTTP error code"""
        return self._code

    def msg(self):
        """Check the internal json variable to return the correct message.
        "msg" is used when we want to store string in the Response
        "detail" comes from Django errors (mainly 401/403)
        otherwise it's a full json response (CDMI for instance)"""
        if "msg" in self._json:
            return self._json["msg"]
        elif "detail" in self._json:
            return self._json["detail"]
        else:
            return self._json

    def json(self):
        """Return a full json message if we are sure we stored a json dict"""
        return self._json

    def __str__(self):
        return "({}, {})".format(self._code, self._json)


class IndigoClient(object):
    """A client to an Indigo archive. Communicate with the archive through HTTP
    REST Api (CDMI for the archive and a simple one for admin operations)"""

    def __init__(self, url):
        """Create a new instance of ``CDMIClient``.

        :arg url: base url of the Indigo archive ("http://127.0.0.1")

        """
        self.url = url
        self.cdmi_url = "{}/api/cdmi".format(url)
        self.admin_url = "{}/api/admin".format(url)
        # pwd should always end with a /
        self._pwd = '/'
        self.auth = None
        self.u_agent = 'Indigo Client {0}'.format(cli.__version__)

    def authenticate(self, username, password):
        """Authenticate the client with ``username`` and ``password``.

        Return success status code for the authentication and a plain text
        message. Possible status code:
          - 0: Successful login
          - 401: Problem with the login/password

        :arg username: username of user to authenticate
        :arg password: plain-text password of user
        :returns: A Response object
        :rtype: Response

        """
        auth = (username, password)
        res = requests.get(self.normalize_admin_url("authenticate"),
                           headers={'user-agent': self.u_agent},
                           auth=auth)
        if res.status_code == 200:
            # authentication ok, keep authentication info for future use
            self.auth = auth
            return Response(0, "Successfully logged in")
        elif res.status_code == 401:
            try:
                val = res.json()
            except ValueError:
                val = "Login credentials not accepted"
            return Response(401, val)
        else:
            return Response(res.status_code, res.content)

    def chdir(self, path):
        """Move into a container at ``path``.

        :arg path: Path of the collection in the archive

        """
        if not path:
            path = u"/"
        elif not path.endswith("/"):
            path = u"{}/".format(path)
        res = self.get_cdmi(path)
        if res.ok():
            cdmi_info = res.json()
            # Check that object is a container
            if not cdmi_info['objectType'] == CDMI_CONTAINER:
                return Response(406,
                                u"{0} isn't a container".format(path))
            if cdmi_info['parentURI'] == "/" and cdmi_info['objectName'] == "Home":
                # root
                self._pwd = u"/"
            else:
                self._pwd = u"{}{}/".format(cdmi_info['parentURI'],
                                            cdmi_info['objectName'])
            return Response(0, "ok")
        else:
            return res

    def add_user_group(self, groupname, ls_user):
        """Add a list of users to a group.

        :arg groupname: Name of the group
        :arg ls_user: List of user names
        :returns: The Response object of the request
        :rtype: requests.Response

        """
        data = {"groupname": groupname,
                "add_users": ls_user}
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url(u"groups/{}".format(groupname))
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=json.dumps(data))
        if res.status_code in [200, 201, 206]:
            return Response(0, res)
        else:
            return Response(res.status_code, res)

    def create_group(self, groupname):
        """Create a new group.

        :arg groupname: Name of the group to create
        :returns: The status code of the Response
        :rtype: int

        """
        data = {"groupname": groupname}
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url("groups")
        res = requests.post(req_url, headers=headers, auth=self.auth,
                            data=json.dumps(data))
        if res.status_code == 201:
            return Response(0, u"Group {} has been created".format(groupname))
        else:
            return Response(res.status_code, res)

    def create_user(self, username, email, is_admin, password):
        """Create a new user.

        :arg username: Name of the user
        :arg email: E-mail of the user
        :arg is_admin: Boolean value, True if the user has administrator access
        :arg password: Plain text password to pass to the archive to be encoded
        :returns: The status code of the Response
        :rtype: int

        """
        data = {"username": username,
                "password": password,
                "email": email,
                "administrator": is_admin}
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url("users")
        res = requests.post(req_url, headers=headers, auth=self.auth,
                            data=json.dumps(data))
        if res.status_code == 201:
            return Response(0, u"User {} has been created".format(username))
        else:
            return Response(res.status_code, res)

    def delete(self, path):
        """Delete a container or a data object.

        .. CAUTION::
            Use with extreme caution. The CDMI Specification (v1.1)
            states that when deleting a container, this includes deletion of
            "all contained children and snapshots", so this is what this
            method will do.

        :arg path: path to delete

        """
        req_url = self.normalize_cdmi_url(path)
        res = requests.delete(req_url, auth=self.auth)
        if res.status_code == 204:
            return Response(0, "ok")
        else:
            return Response(res.status_code, res)

    def get_admin(self, path):
        """Return response for an admin URL.

        :arg path: path to read
        :returns: JSON response
        :rtype: dict

        """
        req_url = self.normalize_admin_url(path)
        headers = {'user-agent': self.u_agent}
        res = requests.get(req_url, headers=headers, auth=self.auth)
        if res.status_code in [400, 401, 403, 404, 406]:
            return Response(res.status_code, res)
        try:
            return Response(0, res.json())
        except ValueError:
            # The API does not appear to return valid JSON
            # It is probably not a CDMI API - this will be a problem!
            return Response(500, "Invalid response format")

    def get_cdmi(self, path):
        """Return CDMI response a container or data object.

        Read the container or data object at ``path`` return the
        CDMI JSON as a dict. If path is empty or not supplied, read the
        current working container.

        :arg path: path to read CDMI
        :returns: (status code, json)
        :rtype: (int, str)

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'X-CDMI-Specification-Version': "1.1"}
        if path.endswith('/'):
            headers['Accept'] = CDMI_CONTAINER
        else:
            headers['Accept'] = CDMI_OBJECT
        res = requests.get(req_url, headers=headers, auth=self.auth, allow_redirects=False)
        if res.status_code in [400, 401, 403]:
            return Response(res.status_code,
                            res.content)
        elif res.status_code in [404, 406]:
            if path.endswith('/'):
                # We remove the trailing '/' in the message
                path = path[:-1]
                msg = u"Cannot access '{0}': No such container".format(path)
                return Response(res.status_code, msg)
            else:
                # Resource doesn't exist, we check if that's a container
                return self.get_cdmi(path + '/')
        elif res.status_code == 502:
            return Response(res.status_code, "Unable to connect")
        elif res.status_code == 302:
            return Response(0, res.json())
        try:
            return Response(0, res.json())
        except ValueError:
            # The API does not appear to return valid JSON
            # It is probably not a CDMI API - this will be a problem!
            return Response(500, "Invalid response format")

    def list_group(self, groupname):
        """Get information about a group.

        {
            'uuid': group.uuid,            # str
            'name': group.name,        # str
            'members': group.members   # list
        }

        :arg groupname: Name of the group to display
        :returns: A dictionary which describes the group
        :rtype: dict

        """
        return self.get_admin(u"groups/{}".format(groupname))

    def list_groups(self):
        """Get a list of existing groups.

        :returns: A list of group names
        :rtype: list

        """
        return self.get_admin("groups")

    def list_user(self, username):
        """Get information about a user.

        {
            'uuid': user.uuid,                    # str
            'username': user.name,                # str
            'email': user.email,                  # str
            'administrator': user.administrator,  # bool
            'active': user.active,                # bool
            'groups': user.groups                 # list
        }

        :arg username: Name of the user to display
        :returns: A dictionary which describes the user
        :rtype: dict

        """
        return self.get_admin(u"users/{}".format(username))

    def list_users(self):
        """Get a list of existing users.

        :returns: A list of user names
        :rtype: list

        """
        return self.get_admin("users")

    def login(self, username, password):
        """Log in client with provided credentials.

        If client is already logged in, log out of current session before
        attempting to log in with new credentials.

        :arg username: username of user to authenticate
        :arg password: plain-text password of user
        :returns: a Response object
        :rtype: Response

        """
        # First log out any existing session
        self.logout()
        # Authenticate using provided credentials
        return self.authenticate(username, password)

    def logout(self):
        """Log out current client session."""
        self.auth = None

    def ls(self, path):
        """List container

        :arg path: Path of the collection in the archive
        :returns: CDMI JSON response
        :rtype: dict

        """
        if not path:
            path = self.pwd()
        elif not path.endswith("/"):
            path = u"{}/".format(path)
        return self.get_cdmi(path)

    def mkdir(self, path):
        """Create a container.

        Create a container at ``path`` and return the CDMI response. User
        ``metadata`` will also be set if supplied.

        :arg path: path to create
        :returns: CDMI JSON response
        :rtype: dict

        """
        if path and not path.endswith('/'):
            path = path + '/'
        return self.put_cdmi(path, {})

    def mod_user(self, username, data):
        """Modify a user, fields to modified are in data which is the JSON
        dictionary which is passed in the request body.

        data example:
        {
            "username": username,
            "password": password,
            "email": email,
            "administrator": is_admin
            "active": is_active
        }

        :arg username: Name of the user to modify
        :arg data: Information for the fields to modify
        :returns: The status code of the Response
        :rtype: int

        """
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url(u"users/{}".format(username))
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=json.dumps(data))
        if res.status_code == 200:
            return Response(0, u"User {} has been modified".format(username))
        else:
            return Response(res.status_code, res)

    def normalize_admin_url(self, path):
        """Normalize URL path.

        :arg path: path relative to current path
        :returns: absolute Admin URL

        """
        # Turn URL path into OS path for manipulation
        mypath = url2pathname(path)
        if not os.path.isabs(mypath):
            mypath = "/" + mypath
        url = self.admin_url + mypath
        return url

    def normalize_cdmi_url(self, path):
        """Normalize URL path relative to current path and return.

        :arg path: path relative to current path
        :returns: absolute CDMI URL

        """
        # Turn URL path into OS path for manipulation
        mypath = url2pathname(path)
        if not os.path.isabs(mypath):
            mypath = os.path.join(url2pathname(self.pwd()), mypath)
        # normalize path
        mypath = os.path.normpath(mypath)
        if path.endswith('/') and not mypath.endswith('/'):
            mypath += '/'
        if isinstance(mypath, unicode):
            mypath = mypath.encode('utf8')
        url = self.cdmi_url + pathname2url(mypath)
        return url

    def put_cdmi(self, path, data):
        """Return JSON response for a PUT to a CDMI URL.

        :arg path: path to put
        :arg data: JSON data to put
        :returns: CDMI JSON response or text response
        :rtype: dict

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'X-CDMI-Specification-Version': "1.1"}
        if path.endswith('/'):
            headers['Content-type'] = CDMI_CONTAINER
        else:
            headers['Content-type'] = CDMI_OBJECT
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=data)
        if res.status_code in [400, 401, 403, 404, 406]:
            return Response(res.status_code, res)
        elif res.status_code == 409:
            return Response(res.status_code,
                            "A resource with this name already exists")
        return Response(0, res)

    def put_http(self, path, data, content_type):
        """Return JSON response for a PUT to a CDMI URL.

        :arg path: path to put
        :arg data: str data to put
        :arg content_type: Content Type for the data
        :returns: text response
        :rtype: str

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'Content-type': content_type}
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=data)
        if res.status_code in [400, 401, 403, 404, 406]:
            return Response(res.status_code, res)
        return Response(0, res)

    def pwd(self):
        """Get and return path of current container.

        :returns: Current working directory
        :rtype: str

        """
        return self._pwd

    def rm_group(self, groupname):
        """Remove a group.

        :arg groupname: Name of the group to remove
        :returns: The status code of the Response
        :rtype: int

        """
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url(u"groups/{}".format(groupname))
        res = requests.delete(req_url, headers=headers, auth=self.auth)
        if res.status_code == 200:
            return Response(0, u"Group {} has been removed".format(groupname))
        else:
            return Response(res.status_code, res)

    def rm_user(self, username):
        """Remove a user.

        :arg username: Name of the user to remove
        :returns: The status code of the Response
        :rtype: int

        """
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url(u"users/{}".format(username))
        res = requests.delete(req_url, headers=headers, auth=self.auth)
        if res.status_code == 200:
            return Response(0, u"User {} has been removed".format(username))
        else:
            return Response(res.status_code, res)

    def rm_user_group(self, groupname, ls_user):
        """Remove a list of users to a group.

        :arg username: Name of the group where the users are removed
        :arg ls_user: List of user names to remove
        :returns: The Response object of the request
        :rtype: requests.Response

        """
        data = {"groupname": groupname,
                "rm_users": ls_user}
        headers = {'user-agent': self.u_agent}
        req_url = self.normalize_admin_url(u"groups/{}".format(groupname))
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=json.dumps(data))
        if res.status_code in [200, 206]:
            return Response(0, res)
        else:
            return Response(res.status_code, res)

    def whoami(self):
        """Return the authenticated user.

        :returns: Current user
        :rtype: str

        """
        if self.auth:
            return self.auth[0]
        else:
            return "Anonymous"

    def open(self, path):
        """Open a URL in stream mode to avoid loading the whole content in
        memory.

        It's possible to iterate ocer the response data with iter_content() or
        iter_lines
        for chunk in res.iter_content(8192):
            # do domething with chunk

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': 'Indigo Client {0}'.format(cli.__version__),
                   'Accept': "application/octet-stream"}
        return requests.get(req_url,
                            headers=headers,
                            auth=self.auth,
                            stream=True)

    def put(self, path, data='', mimetype=None, metadata={}):
        """Create or update a data object.

        Create or update the data object at ``path`` and return the CDMI
        response. Data object content is updated with ``data`` (defaults to
        empty). ``mimetype`` and user ``metadata`` will also be set if
        supplied.

        If ``mimetype`` is not supplied CDMIClient will attempt to do the
        most sensible thing based on the type of ``data`` argument and its
        attributes (e.g. use ``mimetypes`` module to guess for a file-like
        objects).

        :arg path: path to create
        :arg data: content for data object
        :type data: dict (of CDMI JSON) byte string or file-like object
        :arg mimetype: mimetype of data object to create.
        :arg metadata: metadata for object
        :returns: CDMI JSON response
        :rtype: dict

        """
        # Deal with missing mimetype
        if not mimetype:
            type_, enc_ = mimetypes.guess_type(path)
            if not type_:
                mimetype = "application/octet-stream"
            else:
                if enc_ == 'gzip' and type_ == 'application/x-tar':
                    mimetype = "application/x-gtar"
                elif enc_ == 'gzip':
                    mimetype = "application/x-gzip"
                elif enc_ == 'bzip2' and type_ == 'application/x-tar':
                    mimetype = "application/x-gtar"
                elif enc_ == 'bzip2':
                    mimetype = "application/x-bzip2"
                else:
                    mimetype = type_
        # Deal with varying data type
        if isinstance(data, dict):
            data = json.dumps(data)
        elif isinstance(data, unicode):
            data = data.encode('utf-8')
        elif not isinstance(data, (mmap.mmap, basestring)):
            # Read the file-like object as a memory mapped string. Looks like
            # a string, but accesses the file directly. This avoids reading
            # large files into memory
            try:
                data = mmap.mmap(data.fileno(), 0, access=mmap.ACCESS_READ)
            except ValueError:
                # Unable to memory map
                # Simply read in file
                data = data.read()

        if metadata:
            # PUT the data as a CDMI object
            # Create the CDMI Data Object Structure
            d = {'metadata': metadata}
            if data:
                d.update({
                    'value': b64encode(data),
                    'valuetransferencoding': "base64",
                    'mimetype': mimetype,
                })
            data = json.dumps(d)
            # Add the metadata parameters into the URL
#             p = ''.join(["metadata:{0};".format(k)
#                          for k
#                          in metadata])
            #req_url = req_url + '?' + p
            return self.put_cdmi(path, data)
        else:
            # PUT the data in non-CDMI to avoid unnecessary base64 overhead
            #req_url = self.normalize_cdmi_url(path)
            self.put_http(path, data, mimetype)
            #return self.get_cdmi(os.path.split(path)[0])
            return self.get_cdmi(path)
