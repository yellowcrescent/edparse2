#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# radtool - ed2/modules/radtool.py
# edparse2: Radical builder
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

__desc__   = "Radical toolkit: process kanji radical components"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import os
import sys
import re
import codecs
import csv

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *


def run(xconfig):
    """Parse CSV file containing Kangxi Radicals and insert data into Mongo"""
    # get input file
    if xconfig.run.infile:
        cfname = os.path.realpath(xconfig.run.infile)
    else:
        failwith(ER.OPT_MISSING, "Must specify an input filename (CSV file)")

    # connect to mongo
    mdx = mongo(xconfig.mongo.uri)

    # open CSV file
    # csv.reader doesn't have proper unicode support, so we just
    # have to pray that it doesn't die on us
    logthis("Opening CSV file:",suffix=cfname,loglevel=LL.INFO)
    cfhand = open(cfname,'rb')
    crd = csv.reader(cfhand)

    radrgx = re.compile('(?P<mrad>.)\s*(?:\((?P<xrad>[^\)]+)\))?')

    # iterate through and parse each line
    radcnt = 0
    for cline in crd:
        # skip empty lines
        if not len(cline):
            continue

        # [0]number, [1]radical(variants), [2]stroke count, [3]pinyin, [4]sino, [5]hiragana-romaji, [6]hangeul-romaja,
        # [7]english meaning, [8]freq, [9]simplified, [10]examples
        logthis("Radical:",suffix=cline[0],loglevel=LL.VERBOSE)

        # split radical field
        trad = radrgx.match(unicode(cline[1],'utf-8')).groupdict()
        mrad = trad['mrad']
        if trad['xrad']:
            xrad = trad['xrad'].strip().split(",")
        else:
            xrad = []

        # split hiragana-romaji field
        try: (hira,roma) = unicode(cline[5],'utf-8').strip().split('-')
        except: hira = unicode(cline[5],'utf-8').strip()

        # build radical document
        raddoc = {
                    '_id': str(cline[0]),
                    'number': int(cline[0]),
                    'radical': mrad,
                    'alt': xrad,
                    'strokes': int(cline[2]),
                    'radname': {
                                 'ja': hira,
                                 'enm':roma,
                                 'en': cline[7].strip()
                               },
                    'freq': int(cline[8].replace(',',''))
                 }

        mdx.upsert('radical', raddoc['_id'], raddoc)
        radcnt += 1

    # close file & done
    cfhand.close()
    logthis("** Parsing complete. Radicals parsed:",suffix=radcnt,loglevel=LL.INFO)

    return 0
