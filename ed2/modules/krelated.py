#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# krelated - ed2/modules/krelated.py
# edparse2: Related Kanji Builder
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

__desc__   = "Related Kanji Builder"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import os
import sys
import re
import json
import codecs
import copy
import operator
import py2neo

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *


def run(xconfig):
    """
    compile lists of related Kanji using Neo4j
    """
    # check for extra options
    margs = xconfig.run.modargs

    # connect to Mongo
    mgx = mongo(xconfig.mongo.uri)

    # get all kanji from Mongo
    kset = mgx.find("kanji", {}, rdict=True)
    logthis("** Kanji objects:",suffix=len(kset),loglevel=LL.INFO)

    # connect to Neo4j
    try:
        neo = py2neo.Graph(xconfig.neo4j.uri)
        ncount = neo.cypher.execute('MATCH (n) RETURN count(*) AS ncount')[0]['ncount']
    except Exception as e:
        logexc(e, "Failed to connect to Neo4j dataset")
        failwith(ER.PROCFAIL, "Unable to continue. Aborting.")

    logthis("** Nodes in Neo4j dataset:",suffix=ncount,loglevel=LL.INFO)

    # check through all kanji
    hasRelated = 0
    for tkan in kset:
        # get the top 20 highest-scoring related nodes
        trel = getRelated(neo, kset, kset[tkan]['kanji'])
        # update kanji entry in Mongo
        mgx.update_set("kanji", tkan, { 'krelated': trel })
        if len(trel) > 0:
            hasRelated += 1

    logthis("** Complete. Kanji with krelated data:",suffix=hasRelated,loglevel=LL.INFO)


def getRelated(neo,klist,inkanji,limit=20):
    """
    get related kanji for input (inkanji)
    pass in a dict of all kanji (klist), and a Neo4j Graph object (neo)
    returns a dict of top (limit) related kanji
    """
    cto = KCounter()
    cto.clear()
    logthis("** Kanji:",suffix=inkanji,loglevel=LL.VERBOSE)

    # match kanji with radicals in the same position
    radmat = neo.cypher.execute(u"MATCH (k:Kanji)-[e:CONTAINS]-(r:Radical)-[e2:CONTAINS]-(k2:Kanji) WHERE k.kanji = '%s' AND e.position = e2.position RETURN k2.kanji AS kanji, r.radical AS radical, e.position AS position" % (inkanji))
    cto.absorb(radmat,score=5)

    # match kanji with radicals in any position
    radmat = neo.cypher.execute(u"MATCH (k:Kanji)-[e:CONTAINS]-(r:Radical)-[:CONTAINS]-(k2:Kanji) WHERE k.kanji = '%s' RETURN k2.kanji AS kanji, r.radical AS radical, e.position AS position" % (inkanji))
    cto.absorb(radmat,score=2)

    # match kanji with same SKIP code
    radmat = neo.cypher.execute(u"MATCH (k:Kanji)-[:WRITTEN]-(r:Skip)-[:WRITTEN]-(k2:Kanji) WHERE k.kanji = '%s' RETURN k2.kanji AS kanji, r.skip AS skip" % (inkanji))
    cto.absorb(radmat,score=3)

    # match kanji with same meaning/sense keywords
    radmat = neo.cypher.execute(u"MATCH (k:Kanji)-[:MEANS]-(r:Sense)-[:MEANS]-(k2:Kanji) WHERE k.kanji = '%s' RETURN k2.kanji AS kanji, r.sense AS sense" % (inkanji))
    cto.absorb(radmat,score=4)

    # match kanji with same readings
    radmat = neo.cypher.execute(u"MATCH (k:Kanji)-[:READS]-(r:Reading)-[:READS]-(k2:Kanji) WHERE k.kanji = '%s' RETURN k2.kanji AS kanji, r.reading AS reading" % (inkanji))
    cto.absorb(radmat,score=1)

    # get top related
    rtop = cto.sorted()[:limit]
    okan = {}
    for tt in rtop:
        # get crossref data for this kanji
        ttid = u'%x' % (ord(tt[0]))
        tto = klist.get(ttid)
        if tto:
            tjoyo = tto.get('grade',None)
            tjdex = tto.get('jindex',None)
        else:
            tjoyo = None
            tjdex = None
        okan[tt[0]] = { 'score': tt[1], 'joyo': tjoyo, 'jindex': tjdex }
        logthis("-- %s -> %s" % (tt[0],json.dumps(okan[tt[0]])),loglevel=LL.DEBUG)

    return okan


class KCounter(object):
    __kdata = {}
    __mainkey = ''

    def __init__(self,mainkey='kanji'):
        self.__mainkey = mainkey

    def __getitem__(self,aname):
        if self.__kdata.has_key(aname):
            return self.__kdata.get(aname)
        else:
            self.__kdata[aname] = 0
            return self.__kdata[aname]

    def __setitem__(self,aname,aval):
        self.__kdata[aname] = aval

    def __iter__(self):
        for ik,iv in self.__kdata.iteritems():
            yield (ik,iv)

    def __repr__(self):
        return json.dumps(self.__kdata)

    def __str__(self):
        return print_r(self.__kdata)

    def absorb(self,newdata,score=1):
        """absorb 'newdata' into existing data, incrementing by 'score'"""
        for tr in newdata:
            self[tr[self.__mainkey]] += score

    def sorted(self):
        """return sorted list of tuples"""
        return sorted(self.__kdata.items(), key=operator.itemgetter(1), reverse=True)

    def clear(self):
        """clear set"""
        self.__kdata = {}
