#!/usr/bin/python

###############################################################################
### imports
###############################################################################
import os
import sys
import subprocess


###############################################################################
### helper methods
###############################################################################
def check_output(command, shell=False):
    if 'check_output' in dir(subprocess):
        return subprocess.check_output(command, shell=shell)
    process = subprocess.Popen(
        command, shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    out, err = process.communicate()
    retcode = process.poll()
    if err and len(err):
        raise subprocess.CalledProcessError(retcode, cmd=command)
    return out

