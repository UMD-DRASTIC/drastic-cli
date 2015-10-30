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

import contextlib
import errno
import json
import mimetypes
import mmap
import os
import urllib2
import requests
from base64 import b64encode
from urllib import pathname2url, url2pathname

import cli
from cli.errors import (
    HTTPError,
    IndigoClientError,
    IndigoConnectionError,
    NoSuchObjectError,
    ObjectConflictError
)

CDMI_CONTAINER = 'application/cdmi-container'
CDMI_OBJECT = 'application/cdmi-object'


class IndigoRequest(object):

    def __init__(self):
        self.common_headers = {
            'user-agent': 'Indigo Client {0}'.format(cli.__version__)
        }
        self.auth = None


    def add_auth(self, username, password):
        self.auth = (username, password)


    def get(self, url, params={}):
        """Send a get request in HTTP mode"""
        headers = {}
        headers.update(self.common_headers)
        return requests.get(url,
                            params=params,
                            headers=headers,
                            auth=self.auth)

    def get_coll(self, url, params={}):
        """Send a get request in CDMI mode for a container"""
        headers = {'Accept' : CDMI_CONTAINER,
                   'X-CDMI-Specification-Version' : "1.1"}
        headers.update(self.common_headers)
        return requests.get(url,
                            params=params,
                            headers=headers,
                            auth=self.auth)

    def get_resc(self, url, params={}):
        """Send a get request in CDMI mode for a resource"""
        headers = {'Accept' : CDMI_OBJECT,
                   'X-CDMI-Specification-Version' : "1.1"}
        headers.update(self.common_headers)
        return requests.get(url,
                            params=params,
                            headers=headers,
                            auth=self.auth)


    def put(self, url, data, headers, params={}):
        """Send a put request in HTTP mode"""
        head = {}
        head.update(headers)
        head.update(self.common_headers)
        return requests.put(url,
                            data=data,
                            params=params,
                            headers=head,
                            auth=self.auth)


    def put_coll(self, url, data, params={}):
        """Send a put request in CDMI mode for a container"""
        headers = {'X-CDMI-Specification-Version' : "1.1",
                   'Content-type': CDMI_CONTAINER}
        headers.update(self.common_headers)
        return requests.put(url,
                            data=data,
                            params=params,
                            headers=headers,
                            auth=self.auth)


    def put_resc(self, url, data, params={}):
        """Send a put request in CDMI mode for a resource"""
        headers = {'X-CDMI-Specification-Version' : "1.1",
                   'Content-type': CDMI_OBJECT}
        headers.update(self.common_headers)
        return requests.put(url,
                            data=data,
                            params=params,
                            headers=headers,
                            auth=self.auth)


    def delete(self, url):
        return requests.delete(url,
                               headers=self.headers,
                               auth=self.auth)


    def rm_auth(self):
        self.auth = None



class IndigoClient(object):

    def __init__(self, url):
        """Create a new instance of ``CDMIClient``."""
        self.url = url
        self.cdmi_url = "{}/api/cdmi".format(url)
        self.admin_url = "{}/api/admin".format(url)
        # pwd should always end with a /
        self._pwd = '/'
        self.auth = None
        self.u_agent = 'Indigo Client {0}'.format(cli.__version__)


    def authenticate(self, username, password):
        """Authenticate the client with ``username`` and ``password``.

        Return success status for the authentication

        :arg username: username of user to authenticate
        :arg password: plain-text password of user
        :returns: whether or not authentication succeeded
        :rtype: bool

        """
        auth = (username, password)
        res = requests.get(self.normalize_admin_url("authenticate"),
                           headers={'user-agent': self.u_agent},
                           auth=auth)
        if res.status_code == 200:
            # authentication ok, keep authentication info for future use
            self.auth = auth
            return True
        else:
            return False


    def chdir(self, path):
        """Move into a container at ``path``."""
        if not path:
            path = "/"
        elif not path.endswith("/"):
            path = "{}/".format(path)
        cdmi_info = self.get_cdmi(path)
        # Check that object is a container
        if not cdmi_info['objectType'] == CDMI_CONTAINER:
            raise NoSuchObjectError(errno.ENOTDIR,
                                    "{0} not a container".format(path))
        if cdmi_info['parentURI'] == "null":
            # root
            self._pwd = "/"
        else:
            self._pwd = "{}{}/".format(cdmi_info['parentURI'],
                                       cdmi_info['objectName'])


    def delete(self, path):
        """Delete a container or data object.

        .. CAUTION::
            Use with extreme caution. THe CDMI Specification (v1.0.2)
            states that when deleting a container, this includes deletion of
            "all contained children and snapshots", so this is what this
            method will do.

        :arg path: path to delete
        :returns: whether or not the operation succeeded
        :rtype: bool

        """
        req_url = self.normalize_cdmi_url(path)
        res = requests.delete(req_url, auth=self.auth)
        if res.status_code == 404:
            raise NoSuchObjectError(404, path)
        elif res.status_code == 401:
            raise HTTPError(401, "Unauthorized")
        elif res.status_code == 403:
            raise HTTPError(403, "Forbidden")


    def get_cdmi(self, path):
        """Return CDMI response a container or data object.

        Read the container or data object at ``path`` return the
        CDMI JSON as a dict. If path is empty or not supplied, read the
        current working container.

        :arg path: path to read CDMI
        :returns: CDMI JSON response
        :rtype: dict

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'X-CDMI-Specification-Version' : "1.1"
                  }
        if path.endswith('/'):
            headers['Accept'] = CDMI_CONTAINER
        else:
            headers['Accept'] = CDMI_OBJECT
        res = requests.get(req_url, headers=headers, auth=self.auth)
        if res.status_code == 404:
            raise NoSuchObjectError(404, path)
        elif res.status_code == 401:
            raise HTTPError(401, "Unauthorized")
        elif res.status_code == 403:
            raise HTTPError(403, "Forbidden")
        elif res.status_code == 406:
            raise HTTPError(406, "Not Acceptable")
        try:
            return res.json()
        except:
            # The API does not appear to return valid JSON
            # It is probably not a CDMI API - this will be a problem!
            raise IndigoClientError(500, "Invalid response format")



    def getcwd(self):
        """Get and return path of current container."""
        return self.pwd()


    def login(self, username, password):
        """Log in client with provided credentials.

        If client is already logged in, log out of current session before
        attempting to log in with new credentials.
        """
        # First log out any existing session
        self.logout()
        # Authenticate using provided credentials
        return self.authenticate(username, password)


    def logout(self):
        """Log out current client session."""
        self.auth = auth


    def ls(self, path):
        """List container"""
        if not path:
            path = self.pwd()
        elif not path.endswith("/"):
            path = "{}/".format(path)
        return self.get_cdmi(path)


    def mkdir(self, path, metadata={}):
        """Create a container.

        Create a container at ``path`` and return the CDMI response. User
        ``metadata`` will also be set if supplied.

        :arg path: path to create
        :arg metadata: metadata for container
        :returns: CDMI JSON response
        :rtype: dict

        """
        if path and not path.endswith('/'):
            path = path + '/'
        return self.put_cdmi(path, {})


    def normalize_admin_url(self, path):
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
        url = self.cdmi_url + pathname2url(mypath)
        return url


    def put_cdmi(self, path, data):
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'X-CDMI-Specification-Version' : "1.1"
                  }
        if path.endswith('/'):
            headers['Content-type'] = CDMI_CONTAINER
        else:
            headers['Content-type'] = CDMI_OBJECT
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=data)
        if res.status_code == 404:
            raise NoSuchObjectError(404, path)
        elif res.status_code == 401:
            raise HTTPError(401, "Unauthorized")
        elif res.status_code == 403:
            raise HTTPError(403, "Forbidden")
        elif res.status_code == 406:
            raise HTTPError(406, "Not Acceptable")
        elif res.status_code == 400:
            raise HTTPError(400, "Bad Request")
        try:
            return res.json()
        except ValueError:
            return res.text


    def put_http(self, path, data, content_type):
        
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': self.u_agent,
                   'Content-type' : content_type
                  }
        res = requests.put(req_url, headers=headers, auth=self.auth,
                           data=data)
        if res.status_code == 404:
            raise NoSuchObjectError(404, path)
        elif res.status_code == 401:
            raise HTTPError(401, "Unauthorized")
        elif res.status_code == 403:
            raise HTTPError(403, "Forbidden")
        elif res.status_code == 406:
            raise HTTPError(406, "Not Acceptable")
        elif res.status_code == 400:
            raise HTTPError(400, "Bad Request")
        return res.text


    def pwd(self):
        """Get and return path of current container."""
        return self._pwd


    def read_container(self, path=''):
        """Read information and contents for a container.

        Read the container at ``path`` return the CDMI JSON as a dict. If
        path is empty or not supplied, read the current working container.

        :arg path: path to read
        :returns: CDMI JSON response
        :rtype: dict

        """
        if path and not path.endswith('/'):
            path = path + '/'
        return self.get_cdmi(path)


    def read_object(self, path):
        """Read and return the value for a data object.

        Read the data object at ``path`` and return its content.
        """
        with self.open(path) as fh:
            return fh.text


    @contextlib.contextmanager
    def open(self, path):
        """Open a read-only file-like object for a data object.

        Read the data object at ``path`` and return a Response object.

        This is suitable for use in a ``with`` statement context manager::

            c = CDMIClient('https://example.com:443/api/cdmi')
            with c.open('path/to/file') as filelike:
                # Do something with the file
                with open('localfile', 'wb') as fh:
                    fh.write(filelike.text)

        """
        req_url = self.normalize_cdmi_url(path)
        headers = {'user-agent': 'Indigo Client {0}'.format(cli.__version__),
                    'Accept' : "application/octet-stream"}
        with contextlib.closing(requests.get(req_url,
                                             headers=headers,
                                             auth=self.auth,
                                             stream=True)) as fh:
            yield fh


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
        req_url = self.normalize_cdmi_url(path)
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
            if req_url.endswith('/'):
                headers = {'Content-type': CDMI_CONTAINER}
            else:
                headers = {'Content-type': CDMI_DATAOBJECT}
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
            p = ''.join(["metadata:{0};".format(k)
                               for k
                               in metadata
                               ])
            #req_url = req_url + '?' + p
            return self.put_cdmi(path, data)
        else:
            # PUT the data in non-CDMI to avoid unnecessary base64 overhead
            #req_url = self.normalize_cdmi_url(path)
            res = self.put_http(path, data, mimetype)
            return self.get_cdmi(path)

