"""
utils.py:
    Utility functions that don't quite belong anywhere else.

Author: Corwin Brown <corwin@corwinbrown.com>
"""
from __future__ import print_function, absolute_import

import os
import subprocess
from tempfile import NamedTemporaryFile

from errors import MissingAvroToolsException, UnableToParseAvroException


def find_avro_tools():
    """
    Search for `avro-tools` in the system path.
    """
    avro_tools_exe = which('avro-tools')
    if not avro_tools_exe:
        raise MissingAvroToolsException('Avro-Tools must be installed!')

    return avro_tools_exe


def which(exe_to_find, search_paths=None):
    """
    Python implementation of `which`. Useful and crossplatform method
        for discovering if external tools are installed.

    Args:
        exe_to_find (str):      Executable to search for.
        search_path (list):     Paths to search. Defaults to $PATH

    Returns:
        str
    """
    search_paths = search_paths or os.environ.get('PATH').split(os.pathsep)
    for path in search_paths:
        exe_path = os.path.join(path, exe_to_find)
        if os.path.isfile(exe_path) and os.access(exe_path, os.X_OK):
            return exe_path


def convert_avdl_to_avpr(avro_idl_path):
    """
    There is not a python friendly way to interact with Avro IDL
        files, so let's convert it to an Avro Protocol file.

    Args:
        avro_idl_path (str):    Path to Avro IDL file to convert.

    Returns:
        str
    """
    avro_tools_exe = find_avro_tools()
    with NamedTemporaryFile() as temp:
        cmd = [avro_tools_exe, 'idl', avro_idl_path, temp.name]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError:
            raise UnableToParseAvroException(
                'Unable to parse the avro file provided at: "{0}"'
                .format(avro_idl_path))

        # Rewind and read our newly created file.
        temp.seek(0)
        return temp.read()
