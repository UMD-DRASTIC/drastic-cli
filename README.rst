
Indigo Command Line Interface
=============================

Description
-----------

A Python client and command-line tool for the Indigo digital archive. Also
includes a rudimentary client for any CDMI enabled cloud storage.

After Installation_, connect to an Indigo archive::

    indigo init --api=https://indigo.example.com/api/cdmi

(or if authentication is required by the archive)::

    indigo init --api=https://indigo.example.com/api/cdmi --username=USER --password=PASS

Show current working container::

    indigo pwd

List a container::

    indigo ls [name]

Move to a new container::

    indigo cd subdir
    ...
    indigo cd ..  # back up to parent

Create a new container::

    indigo mkdir new

Put a local file::

    indigo put source.txt
    ...
    indigo put source.txt destination.txt  # Put to a different name remotely

Provide the MIME type of the object (if not supplied ``indigo put`` will attempt
to guess)::

     indigo put --mimetype="text/plain" source.txt

Fetch a data object from the archive to a local file::

    indigo get source.txt

    indigo get source.txt destination.txt  # Get with a different name locally

    indigo get --force source.txt  # Overwrite an existing local file

Remove an object or a container::

    indigo rm file.txt

Close the current session to prevent unauthorized access::

    indigo exit


Advanced Use - Metadata
~~~~~~~~~~~~~~~~~~~~~~~

Set (overwrite) a metadata value for a field::

    indigo meta set file.txt "org.dublincore.creator" "S M Body"
    indigo meta set . "org.dublincore.title" "My Collection"

Add another value to an existing metadata field::

    indigo meta add file.txt "org.dublincore.creator" "A N Other"

List metadata values for all fields::

    indigo meta ls file.txt

List metadata value(s) for a specific field::

    indigo meta ls file.txt org.dublincore.creator

Delete a metadata field::

    indigo meta rm file.txt "org.dublincore.creator"

Delete a specific metadata field with a value::

    indigo meta rm file.txt "org.dublincore.creator" "A N Other"


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

