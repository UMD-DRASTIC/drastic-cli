
Indigo Command Line Interface
=============================

Description
-----------

A Python client and command-line tool for the Indigo digital archive. Includes 
a rudimentary client for any CDMI enabled cloud storage.

After Installation_, connect to an Indigo archive::

    indigo init --url=http://indigo.example.com

(or if authentication is required by the archive)::

    indigo init --url=http://indigo.example.com --username=USER --password=PASS

(if you don't want to pass the password in the command line it will be asked if
you don't provide the --password option)
    indigo init --url=http://indigo.example.com --username=USER

Close the current session to prevent unauthorized access::

    indigo exit

Show current working container::

    indigo pwd

Show current authenticated user::

    indigo whoami

List a container::

    indigo ls <path>

List a container wit ACL information::

    indigo ls -a <path>

Move to a new container::

    indigo cd <path>
    ...
    indigo cd ..  # back up to parent

Create a new container::

    indigo mkdir <path>

Put a local file, with eventually a new name::

    indigo put <src>
    ...
    indigo put <src> <dst>

Create a reference object::

    indigo put --ref <url> <dest>

Provide the MIME type of the object (if not supplied ``indigo put`` will attempt
to guess)::

     indigo put --mimetype="text/plain" <src>

Fetch a data object from the archive to a local file::

    indigo get <src>

    indigo get <src> <dst>

    indigo get --force <src> # Overwrite an existing local file

Get the CDMI json dict for an object or a container

    indigo cdmi <path>

Remove an object or a container::

    indigo rm <src>

Add or modify an ACL to an object or a container::

    indigo chmod <path> (read|write|null) <group>


Advanced Use - Metadata
~~~~~~~~~~~~~~~~~~~~~~~

Set (overwrite) a metadata value for a field::

    indigo meta set <path> "org.dublincore.creator" "S M Body"
    indigo meta set . "org.dublincore.title" "My Collection"

Add another value to an existing metadata field::

    indigo meta add <path> "org.dublincore.creator" "A N Other"

List metadata values for all fields::

    indigo meta ls <path>

List metadata value(s) for a specific field::

    indigo meta ls <path> org.dublincore.creator

Delete a metadata field::

    indigo meta rm <path> "org.dublincore.creator"

Delete a specific metadata field with a value::

    indigo meta rm <path> "org.dublincore.creator" "A N Other"


Advanced Use - Administration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

List existing users::

    indigo admin lu

List information about a user::

    indigo admin lu <name>

List existing groups::

    indigo admin lg

List information about a group::

    indigo admin lg <name>

Create a user::

    indigo admin mkuser [<name>]

Modify a user::

    indigo admin moduser <name> (email | administrator | active | password) [<value>]

Remove a user::

    indigo admin rmuser [<name>]

Create a group::

    indigo admin mkgroup [<name>]

Remove a group::

    indigo admin rmgroup [<name>]

Add user(s) to a group::

    indigo admin atg <name> <user> ...

Remove user(s) from a group::

    indigo admin rtg <name> <user> ...



Installation
------------

Create And Activate A Virtual Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    $ virtualenv ~/ve/indigo/cli<version>
    ...
    $ source ~/ve/indigo/cli/bin/activate


Install Dependencies
~~~~~~~~~~~~~~~~~~~~
::

    pip install -r requirements.txt


Install Indigo Client
~~~~~~~~~~~~~~~~~~~~
::

    pip install -e .


Detailed OSX install  commands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
::

    sudo easy_install virtualenv      # virtualenv installs pip
    python -m virtualenv ~/ve/indigoclient<version>
    source ~/ve/indigoclient<version>/bin/activate
    pip install -r requirements.txt
    pip install -e .


License
-------

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

