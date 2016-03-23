#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# kvgbuild - ed2/modules/kvgbuild.py
# edparse2: KVG Builder
#
# Process KanjiVG SVG documents to build Stroke Order diagrams and extract
# positional radical data
#
# @author   J. Hipps <jacob@ycnrg.org>
# @repo     https://bitbucket.org/yellowcrescent/edparse2
#
# Copyright (c) 2015-2016 J. Hipps / Neo-Retro Group
#
# KanjiVG <http://kanjivg.tagaini.net/>
# Copyright (c) 2009-2011 Ulrich Apel, used under license.
#
# https://ycnrg.org/
# https://hotarun.co/
#
###############################################################################

__desc__   = "Kanji SVG builder (KanjiVG)"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import sys
import os
import subprocess
import re
import json
import signal
import optparse
import operator
import time
import codecs
from xml.dom import minidom
from bs4 import BeautifulSoup

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *

radpal = [
            'rgb(215,75,75)',   # soft red
            'rgb(41,128,185)',  # deep blue
            'rgb(255,84,13)',   # burnt orange
            'rgb(4,140,102)',   # seafoam green
            'rgb(53,75,94)',    # soft navy blue
            'rgb(69,33,150)',   # violet
            'rgb(231,76,70)',   # red
            'rgb(255,176,59)',  # yellow
            'rgb(44,89,81)',    # teal
            'rgb(111,115,110)', # grey
            'rgb(232,63,106)',  # pink
            'rgb(113,224,255)', # baby blue
            'rgb(255,233,102)', # dull yellow
            'rgb(238,129,255)', # rose
            'rgb(169,132,57)',  # light brown
            'rgb(207,102,95)',  # salmon pink
            'rgb(127,64,152)',  # purple
            'rgb(63,191,144)',  # cyan
            'rgb(255,0,153)',   # hot pink
            'rgb(61,63,26)',    # puke green
            'rgb(173,69,27)',   # woody red
            'rgb(123,169,195)', # dull blue
            'rgb(107,255,162)', # light green

         ]
snumpal = 'rgb(92,92,92)'

def run(xconfig):
    """kvgbuild entry point"""
    # get input directory
    if xconfig.run.infile:
        inpath = os.path.realpath(xconfig.run.infile)
        if not os.path.exists(inpath):
            failwith(ER.NOTFOUND, "Specified directory not found")
        elif not os.path.isdir(inpath):
            failwith(ER.CONF_BAD, "Must specify a directory, not a file")

    # get output directory
    if xconfig.run.output:
        outpath = os.path.realpath(xconfig.run.output)
        if not os.path.exists(outpath):
            failwith(ER.NOTFOUND, "Specified directory not found")
        elif not os.path.isdir(outpath):
            failwith(ER.CONF_BAD, "Must specify a directory, not a file")

    # check for extra options
    margs = xconfig.run.modargs
    if "norender" in margs:
        render = False
    else:
        render = True

    kflist = os.listdir(inpath)
    ftots = len(kflist)
    fmatchy = re.compile('^[0-9a-f]{5}.svg$')
    ftit = 0

    # build PNG images
    if render:
        logthis("Rendering stroke-order diagrams...",loglevel=LL.INFO)
        for tf in kflist:
            ftit += 1
            if fmatchy.match(tf):
                logthis("[ %i / %i ] Processing" % (ftit,ftots),suffix=tf,loglevel=LL.VERBOSE)
                thiskvg = openKvg(inpath+'/'+tf)
                colorGroups(thiskvg)
                thispng = inkscapeKvg(thiskvg)

                pf = open(outpath + '/' + tf.split('.')[0] + '.png','w')
                pf.write(thispng)
                pf.close()

                # destroy XML DOM object
                thiskvg.unlink()

    # connect to mongo
    logthis("Connecting to",suffix=xconfig.mongo.uri,loglevel=LL.INFO)
    mdx = mongo(xconfig.mongo.uri)

    # parse radical data
    logthis("Parsing kanji radical data...",loglevel=LL.INFO)
    for tf in kflist:
        ftit += 1
        if fmatchy.match(tf):
            radlist = {}
            kid = os.path.splitext(os.path.split(tf)[1])[0]
            logthis("[ %i / %i ] Processing" % (ftit,ftots),suffix="%s [%s]" % (tf,kid),loglevel=LL.VERBOSE)

            # read
            with codecs.open(inpath+'/'+tf,'r','utf-8') as f:
                thiskvg = f.read()

            # parse with BS4 & lxml
            bs = BeautifulSoup(thiskvg,'xml')
            kvg = bs.find('g', { 'id': "kvg:"+kid })

            # weed out the nasties
            if not kvg:
                continue
            kanji = kvg.attrs.get('element',None)
            if not kanji:
                continue
            if not kvg:
                logthis("XPath /svg/g/g[id:%s] not found" % (kid),loglevel=LL.ERROR)
                continue

            # parse attributes
            for tg in kvg.find_all('g'):
                trad = tg.attrs
                if trad.has_key('id'): del(trad['id'])

                if not trad.has_key('original'):
                    if trad.has_key('element'):
                        trad['original'] = tg.attrs['element']
                    else:
                        logthis("Skipping:",suffix=trad,loglevel=LL.DEBUG)
                        continue

                # get position from parent if not specified
                if not trad.has_key('position'):
                    if tg.parent.attrs.get('position',None):
                        trad['position'] = tg.parent.attrs.get('position',None)
                    else:
                        if tg.parent.parent.attrs.has_key('position'):
                            trad['position'] = tg.parent.parent.attrs.get('position',None)

                radlist[trad['original']] = trad

            mdx.update_set('kanji', "%x" % ord(kanji), { 'xrad': radlist } )
            logthis("** Committed entry:\n",suffix=print_r(radlist),loglevel=LL.DEBUG)



def openKvg(infile):
    # parse input file
    kvg = minidom.parse(infile)
    return kvg

def colorGroups(kvg):
    """colorize radicals & stroke groups"""
    # get groups <g>
    ggs = kvg.getElementsByTagName('g')
    rurad = re.compile('kvg:[0-9a-f]{5}-g[0-9]+$')
    rupath = re.compile('^kvg:StrokePaths')
    runumb = re.compile('^kvg:StrokeNumbers')
    cindex = 0
    strokes = 0
    # iterate through each grouping and style it up!
    for g in ggs:
        if rurad.match(g.getAttribute('id')):
            # make sure this group doesn't have any child groups
            if not len(g.getElementsByTagName('g')):
                # add stroke color to each radical or stroke set
                newstyle = 'stroke:' + radpal[cindex] + ';'
                oldstyle = g.getAttribute('style')
                g.setAttribute('style',oldstyle+newstyle)
                cindex += 1
                # count strokes
                strokes += len(g.getElementsByTagName('path'))

    for g in ggs:
        if rupath.match(g.getAttribute('id')):
            # set stroke width
            if strokes < 10: swidth = '4'
            elif strokes < 20: swidth = '3'
            else: swidth = '2'
            newstyle = 'stroke-width:'+swidth+'px;'
            oldstyle = g.getAttribute('style')
            g.setAttribute('style',oldstyle+newstyle)
        elif runumb.match(g.getAttribute('id')):
            # set stroke number styles
            newstyle = 'fill:'+snumpal+';font-size:8px;font-family:"Droid Sans";'
            oldstyle = g.getAttribute('style')
            g.setAttribute('style',oldstyle+newstyle)

def inkscapeKvg(kvg,width=150):
    inky = subprocess.Popen(['/usr/bin/inkscape','-z','-e','/dev/stdout','-w',unicode(width),'/dev/stdin'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    tainted = inky.communicate(unicode(kvg.toxml()).encode('utf-8'))[0]
    # remove inkscape's taint from the top of the file
    pngout = tainted.split('stdout\n')[1]
    return pngout
