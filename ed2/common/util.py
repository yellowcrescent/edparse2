#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# util - ed2/common/util.py
# edparse2: Common utility functions
#
# @author   J. Hipps <jacob@ycnrg.org>
# @repo     https://bitbucket.org/yellowcrescent/edparse2
#
# Copyright (c) 2015-2016 J. Hipps / Neo-Retro Group
#
# https://ycnrg.org/
# https://hotarun.co/
#
###############################################################################

import __main__
import sys
import os
import re
import json
import time
import subprocess
import socket
from datetime import datetime

from ..common.logthis import *


class XConfig(object):
    """
    Config management object; allow access via attributes or items
    """
    __data = {}

    def __init__(self, idata):
        self.__data = idata

    def __getattr__(self, aname):
        if self.__data.has_key(aname):
            if isinstance(self.__data[aname], dict):
                return XConfig(self.__data[aname])
            else:
                return self.__data[aname]
        else:
            raise KeyError(aname)

    def __getitem__(self, aname):
        return self.__getattr__(aname)

    def __str__(self):
        return print_r(self.__data)

    def __repr__(self):
        return print_r(self.__data)


def rexec(optlist,supout=False):
    """
    execute command; input a list of options; if `supout` is True, then suppress stderr
    """
    logthis("Executing:",suffix=optlist,loglevel=LL.DEBUG)
    try:
        if supout:
            fout = subprocess.check_output(optlist,stderr=subprocess.STDOUT)
        else:
            fout = subprocess.check_output(optlist)
    except subprocess.CalledProcessError as e:
        logthis("exec failed:",suffix=e,loglevel=LL.ERROR)
        fout = None
    return fout


def fmtsize(insize,rate=False,bits=False):
    """
    format human-readable file size and xfer rates
    """
    onx = float(abs(insize))
    for u in ['B','K','M','G','T','P']:
        if onx < 1024.0:
            tunit = u
            break
        onx /= 1024.0
    suffix = ""
    if u != 'B': suffix = "iB"
    if rate:
        if bits:
            suffix = "bps"
            onx *= 8.0
        else:
            suffix += "/sec"
    if tunit == 'B':
        ostr = "%3d %s%s" % (onx,tunit,suffix)
    else:
        ostr = "%3.01f %s%s" % (onx,tunit,suffix)
    return ostr


def git_info():
    """
    retrieve git info
    """
    # change to directory of rainwatch
    lastpwd = os.getcwd()
    os.chdir(os.path.dirname(os.path.realpath(__main__.__file__)))

    # run `git show`
    ro = rexec(['/usr/bin/git','show'])

    # change back
    os.chdir(lastpwd)

    # set defaults
    rvx = { 'ref': None, 'sref': None, 'date': None }
    if ro:
        try:
            cref = re.search('^commit\s*(.+)$',ro,re.I|re.M).group(1)
            cdate = re.search('^Date:\s*(.+)$',ro,re.I|re.M).group(1)
            rvx = { 'ref': cref, 'sref': cref[:8], 'date': cdate }
        except Exception as e:
            logexc(e, "Unable to parse output from git")

    return rvx


def setMerge(a,b):
    """merge two dicts; create a list when multiple (different) values exist"""
    # create set A
    seta = set()
    for ak,av in a.iteritems(): seta.add('%s\\\\\\%s' % (ak,json.dumps(av)))

    # create set B
    setb = set()
    for bk,bv in b.iteritems(): setb.add('%s\\\\\\%s' % (bk,json.dumps(bv)))

    # create union
    setc = list(seta.union(setb))

    # rebuild output dict
    outd = {}
    for tx in setc:
        tk,tv = tx.split('\\\\\\')
        tvd = json.loads(tv)
        logthis(">[%s]" % (tk),suffix=tvd,loglevel=LL.DEBUG2)
        if outd.has_key(tk):
            if isinstance(outd[tk], list):
                if isinstance(tvd, list):
                    outd[tk] = outd[tk] + tvd
                else:
                    outd[tk] = outd[tk] + [ tvd ]
            else:
                if isinstance(tvd, list):
                    outd[tk] = [ outd[tk] ] + tvd
                else:
                    outd[tk] = [ outd[tk] ] + [ tvd ]
        else:
            outd[tk] = tvd

    return outd
