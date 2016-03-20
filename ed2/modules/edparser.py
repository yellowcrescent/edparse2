#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# edparser - ed2/modules/edparser.py
# edparse2: EDRDG File Parser
#
# @author   J. Hipps <jacob@ycnrg.org>
# @repo     https://bitbucket.org/yellowcrescent/edparse2
#
# Copyright (c) 2013-2016 J. Hipps / Neo-Retro Group
#
# https://ycnrg.org/
# https://hotarun.co/
#
###############################################################################

__desc__   = "EDRDG file parser"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import os
import sys
import re
import codecs

from bs4 import BeautifulSoup

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *

targets = ["jmdict","jmnedict","kradfile","radkfile","kradfile2","kanjidic"]

krdex  = {}
kdex   = {}
jmdict = {}
nedict = {}


def run(xconfig):
	"""edparser entry point"""
	# get input directory
	if xconfig.run.infile:
		indir = os.path.realpath(xconfig.run.infile)
		if not os.path.exists(indir):
			failwith(ER.NOTFOUND, "Specified directory not found")
		elif not os.path.isdir(indir):
			failwith(ER.CONF_BAD, "Must specify a directory, not a file")

	# check for extra options
	margs = xconfig.run.modargs

	# find files for conversion
	logthis(">> Using directory:",suffix=indir,loglevel=LL.VERBOSE)
	tmap = {}
	for tf in os.listdir(indir):
		matched = False
		for tm in targets:
			if tmap.has_key(tm):
				continue
			if re.match("^"+tm+".*$", tf, re.I):
				tmap[tm] = os.path.realpath(indir+'/'+tf)
				matched = True
				logthis("Found match for %s:" % (tm),suffix=tmap[tm],loglevel=LL.VERBOSE)
				break
		if not matched:
			logthis("File skipped:",suffix=tf,loglevel=LL.DEBUG)

	# ensure everybody is here
	if len(set(targets) - set(tmap)) > 0:
		logthis("!! Missing required files:",suffix=', '.join(set(targets) - set(tmap)),loglevel=LL.ERROR)
		failwith(ER.NOTFOUND, "All files must be present to build crossrefs. Unable to continue.")

	### Parse input files

	# parse kradfile & kradfile2
	parse_kradfile(tmap['kradfile'])
	parse_kradfile(tmap['kradfile2'])

	# TODO: parse kanjidic2
	# TODO: parse jmdict
	# TODO: parse jmnedict


def parse_kradfile(krfile):
	"""
	Parse KRADFILE & KRADFILE2
	These are EUC-JP (JIS X 0212) encoded files
	"""
	global krdex
	logthis("Parsing file",suffix=krfile,loglevel=LL.INFO)

	# convert file from EUC-JP to Unicode
	with codecs.open(krfile,'r','euc-jp') as f:
		krraw = f.read()

	# parse line-by-line
	kc = 0
	for tline in krraw.splitlines():
		# skip empty lines & comments
		if tline[0] == '#' or tline[0] == ' ':
			continue
		# split left/right
		try:
			rkan,rrads = tline.split(':')
		except:
			continue
		tkanji = rkan.strip()
		trads = rrads.strip().split()
		krdex[tkanji] = trads
		kc += 1

	logthis("** KRADFILE Kanji parsed:",suffix=kc,loglevel=LL.INFO)
