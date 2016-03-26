#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# edfetch - ed2/modules/edfetch.py
# edparse2: Fetch copy of latest EDRDG source files
#
# @author   J. Hipps <jacob@ycnrg.org>
# @repo     https://bitbucket.org/yellowcrescent/edparse2
#
# Copyright (c) 2016 J. Hipps / Neo-Retro Group
#
# https://ycnrg.org/
# https://hotarun.co/
#
###############################################################################

__desc__   = "Fetch copy of latest EDRDG source files"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import os
import sys
import re
import codecs
import requests

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *

fmanifest = [
                { 'url': 'http://ftp.monash.edu.au/pub/nihongo/JMdict.gz' },
                { 'url': 'http://ftp.monash.edu.au/pub/nihongo/JMnedict.xml.gz' },
                { 'url': 'http://ftp.monash.edu.au/pub/nihongo/kanjidic2.xml.gz' },
                { 'url': 'http://ftp.monash.edu.au/pub/nihongo/kradfile.gz', 'encoding': 'euc-jp' },
                { 'url': 'http://www.kanjicafe.com/downloads/kradfile2.gz', 'encoding': 'euc-jp' },
                { 'url': 'http://ftp.monash.edu.au/pub/nihongo/examples.utf.gz' }
            ]

def run(xconfig):
    """entry point"""
    # get output directory
    if xconfig.run.output:
        outpath = os.path.realpath(xconfig.run.output)
        if not os.path.exists(outpath):
            failwith(ER.NOTFOUND, "Specified directory not found")
        elif not os.path.isdir(outpath):
            failwith(ER.CONF_BAD, "Must specify a directory, not a file")

    logthis("Output directory:",suffix=outpath,loglevel=LL.INFO)

    # fetch files
    fails = 0
    for tf in fmanifest:
        tfo = re.match('^https?://.+/([^/]+)\.gz$', tf['url']).group(1)
        if not fetch(tf['url'], outpath+'/'+tfo, tf.get('encoding','utf-8')):
            fails += 1

    return fails


def fetch(url,outfile,encoding='utf-8'):
    """
    fetch a file from a URL and save it; gzip'd files will be decoded automagically
    """
    ofrp = os.path.realpath(os.path.expanduser(outfile))
    logthis("Fetching:",suffix=url,loglevel=LL.INFO)

    # retrieve file and decompress it on-the-fly (as long as it has a Content-Encoding gzip header)
    try:
        r = requests.get(url)
        r.raise_for_status()
    except Exception as e:
        logexc(e, "Failed to retrieve file")
        return False

    # coerce to the correct encoding (either UTF-8 or EUC-JP... usually)
    r.encoding = encoding

    try:
        with codecs.open(ofrp,'w',encoding) as f:
            f.write(r.text)
    except Exception as e:
        logexc(e, "Failed to write output to %s" % (outfile))
        return False

    logthis(">> Wrote output to",suffix=ofrp,loglevel=LL.INFO)
    return True
