
# Drastic Command Line Interface

## Description

A Python client and command-line tool for the Drastic digital archive. Includes
a rudimentary client for any CDMI enabled cloud storage.

## Basic Commands

* Connect to a Drastic archive:

    drastic init --url=http://drastic.example.com

* Connect and authenticate to a Drastic archive:

    drastic init --url=http://drastic.example.com --username=USER --password=PASS

You will be prompted for a password if you don't put that option on the command line.

    drastic init --url=http://drastic.example.com --username=USER

* Close the current session to prevent unauthorized access:

    drastic exit

* Show current working container:

    drastic pwd

* Show current authenticated user:

    drastic whoami

* List a container:

    drastic ls <path>

* List a container wit ACL information:

    drastic ls -a <path>

* Move to a new container:

    drastic cd <path>
    ...
    drastic cd ..  # back up to parent

* Create a new container:

    drastic mkdir <path>

* Put a local file, optionally with a new name:

    drastic put <src>
    ...
    drastic put <src> <dst>

* Create a reference object, optionally include the MIME type of the object (if not supplied ``drastic put`` will attempt
to guess):

    drastic put --ref <url> <dest>
    ...
    drastic put --mimetype="text/plain" <src>

* Fetch a data object from the archive to a local file:

    drastic get <src>

    drastic get <src> <dst>

    drastic get --force <src>  # Overwrite an existing local file

* Get the CDMI json dict for an object or a container:

    drastic cdmi <path>

* Remove an object or a container:

    drastic rm <src>

* Add or modify an ACL to an object or a container:

    drastic chmod <path> (read|write|null) <group>


## Advanced Use - Metadata

* Set (overwrite) a metadata value for a field:

    drastic meta set <path> "org.dublincore.creator" "S M Body"
    drastic meta set . "org.dublincore.title" "My Collection"

* Add another value to an existing metadata field:

    drastic meta add <path> "org.dublincore.creator" "A N Other"

* List metadata values for all fields:

    drastic meta ls <path>

* List metadata value(s) for a specific field:

    drastic meta ls <path> org.dublincore.creator

* Delete a metadata field:

    drastic meta rm <path> "org.dublincore.creator"

* Delete a specific metadata field with a value:

    drastic meta rm <path> "org.dublincore.creator" "A N Other"


## Advanced Use - Administration

* List existing users:

    drastic admin lu

* List information about a user:

    drastic admin lu <name>

* List existing groups:

    drastic admin lg

* List information about a group:

    drastic admin lg <name>

* Create a user:

    drastic admin mkuser [<name>]

* Modify a user:

    drastic admin moduser <name> (email | administrator | active | password) [<value>]

* Remove a user:

    drastic admin rmuser [<name>]

* Create a group:

    drastic admin mkgroup [<name>]

* Remove a group:

    drastic admin rmgroup [<name>]

* Add user(s) to a group:

    drastic admin atg <name> <user> ...

* Remove user(s) from a group:

    drastic admin rtg <name> <user> ...

## Installation

These steps work well on a Mac or on Linux. Windows users who are not Python users
will have been luck with the Windows Installer, available on the [releases page](https://github.com/UMD-DRASTIC/drastic-cli/releases).

### Create And Activate A Virtual Environment

This step is optional. You can also install the DRAS-TIC command-line tool without a virtual environment.

    $ virtualenv ~/ve/drastic/cli<version>
    ...
    $ source ~/ve/drastic/cli/bin/activate

### Install DRAS-TIC Command

1. Download the DRAS-TIC Command-Line project *source code*
as a [release archive](https://github.com/UMD-DRASTIC/drastic-cli/releases) (ZIP) or clone the project with git:

    $ git clone https://github.com/UMD-DRASTIC/drastic-cli.git

1. if you downloaded the ZIP file, unzip the project source code:

    $ unzip 1.0.0.zip

1. Change to the project folder:

    $ cd drastic-cli

1. Install Dependencies:

    $ pip install -r requirements.txt

1. Install Drastic Client:
    $ pip install -e .

1. Try a Drastic command:

    $ drastic init --url=http://ciber.umd.edu --user=joe
