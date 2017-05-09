# Building the DRAS-TIC Command-Line Interface for Windows and Mac OS X

These are the instructions for building the DRAS-TIC distributions or installers that are end-user installable on Windows. These are packaged with the Python runtime and dependencies, for independent installation on the operating system. These distributions are not recommended for use in Python code, as a DRAS-TIC client library or modules. For that type of use, try the pip install command.

This build works on Ubuntu, Debian/Linux generally, and Mac OSX. So you do not need to run MS Windows to build for that platform.

## PyNsist: Python NullSoft Installer

The distribution for Windows uses a NullSoft Installer (NSIS), which creates an executable (.exe) that installs all necessary components, including Python. It also includes an uninstaller. After install, a "drastic.exe" is placed in the DRAS-TIC folder and can be run as "drastic" from the Windows command-line. The enclosing "bin" folder may need to be added to the PATH system variable.

### Steps to Build

1. Install the NSIS package (NullSoft Installer System).

    $ sudo apt-get install nsis

1. Install the pynsist Python module.

    $ sudo pip install pynsist

1. Clean the build and dist folders.

    $ cd drastic-cli
    $ rm -rf build dist

1. Run the Pynsist build tool.

    $ pynsist pynsist_install.cfg

1. Find Windows installer "DRAS-TIC_1.0.exe" in the "build/nsis" folder.
