#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# kgraph - ed2/modules/kgraph.py
# edparse2: Kanji graph builder for Neo4j
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

__desc__   = "Kanji graph builder for Neo4j"
__author__ = "J. Hipps <jacob@ycnrg.org>"

import __main__
import os
import sys
import re
import json
import codecs
import copy
import py2neo

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *


def run(xconfig):
    """
    build graph in Neo4j based on the Kanji dataset
    """
    # check for extra options
    margs = xconfig.run.modargs

    # connect to Mongo
    mgx = mongo(xconfig.mongo.uri)

    # get all kanji from Mongo
    kset = mgx.find("kanji", {})
    logthis("** Kanji objects:",suffix=len(kset),loglevel=LL.INFO)

    # connect to Neo4j
    try:
        neo = py2neo.Graph(xconfig.neo4j.uri)
        ncount = neo.cypher.execute('MATCH (n) RETURN count(*)')
    except Exception as e:
        logexc(e, "Failed to connect to Neo4j dataset")
        failwith(ER.PROCFAIL, "Unable to continue. Aborting.")

    # if 'clear' is passed as an extra parg, then drop all existing nodes/rels
    if 'clear' in margs:
        logthis("Deleting existing data...",loglevel=LL.INFO)
        neo.cypher.execute("MATCH (n) DETACH DELETE n")

    # create node constraints
    logthis("Creating constraints...",loglevel=LL.VERBOSE)
    neo.cypher.execute("CREATE CONSTRAINT ON (k:Kanji) ASSERT k.kanji IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (r:Radical) ASSERT r.radical IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (s:Sense) ASSERT s.sense IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (r:Reading) ASSERT r.reading IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (g:Joyo) ASSERT g.joyo IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (g:Jlpt) ASSERT g.jlpt IS UNIQUE")
    neo.cypher.execute("CREATE CONSTRAINT ON (k:Skip) ASSERT k.skip IS UNIQUE")


    # Build nodes & relationships
    logthis("** Building graph...",loglevel=LL.INFO)
    for kk,tk in kset.iteritems():
        logthis(">>>------[ %5d ] Kanji node <%s> -----" % (kk,tk['kanji']),loglevel=LL.DEBUG)

        # Kanji
        try: freq = int(tk['freq'])
        except: freq = 0
        knode = py2neo.Node("Kanji", kanji=tk['kanji'], ucs=tk['_id'], freq=freq)

        # Radicals
        xnodes = []
        xrels = []
        if tk.has_key('xrad') and len(tk['xrad']) > 0:
            for tr,tv in tk['xrad'].iteritems():
                # check if a radical exists in db.radical
                rrad = mgx.findOne("radical", { "radical": tr })
                xrad = {}
                if rrad:
                    xrad = { "rad_id": rrad['_id'], "alt": rrad['alt'], "radname": rrad['radname']['ja'], "radname_en": rrad['radname']['en'] }
                else:
                    rrad = mgx.findOne("kanji", { "kanji": tr })
                    if rrad:
                        # Created Kanji-Kanji relationship
                        xrad = False
                        try: freq = int(rrad['freq'])
                        except: freq = 0
                        xnodes.append(py2neo.Node("Kanji", kanji=rrad['kanji'], ucs=rrad['_id'], freq=freq))
                        xrels.append(py2neo.Relationship(knode, "CONTAINS", xnodes[-1], position=tv.get('position',None)))
                    else:
                        xrad = { "non_standard": True }
                if xrad:
                    xnodes.append(py2neo.Node("Radical", radical=tr, **xrad))
                    xrels.append(py2neo.Relationship(knode, "CONTAINS", xnodes[-1], position=tv.get('position',None)))

        elif tk.has_key('krad'):
            for tr in tk['krad']:
                # check if a radical exists in db.radical
                rrad = mgx.findOne("radical", { "radical": tr })
                xrad = {}
                if rrad:
                    xrad = { "rad_id": rrad['_id'], "alt": rrad['alt'], "radname": rrad['radname']['ja'], "radname_en": rrad['radname']['en'] }
                else:
                    rrad = mgx.findOne("kanji", { "kanji": tr })
                    if rrad:
                        # Created Kanji-Kanji relationship
                        xrad = False
                        try: freq = int(rrad['freq'])
                        except: freq = 0
                        xnodes.append(py2neo.Node("Kanji", kanji=rrad['kanji'], ucs=rrad['_id'], freq=freq))
                        xrels.append(py2neo.Relationship(knode,"CONTAINS",xnodes[-1]))
                    else:
                        xrad = { "non_standard": True }
                if xrad:
                    xnodes.append(py2neo.Node("Radical", radical=tr, **xrad))
                    xrels.append(py2neo.Relationship(knode, "CONTAINS", xnodes[-1]))

        # Senses
        if tk.has_key('meaning') and tk['meaning'].get('en'):
            for ts in tk['meaning']['en']:
                xnodes.append(py2neo.Node("Sense", sense=ts, lang="en"))
                xrels.append(py2neo.Relationship(knode, "MEANS", xnodes[-1]))

        # Readings (on-yomi, kun-yomi, nanori)
        if tk.has_key('reading'):
            if tk['reading'].has_key('ja_on'):
                for tr in tk['reading']['ja_on']:
                    xnodes.append(py2neo.Node("Reading", reading=tr))
                    xrels.append(py2neo.Relationship(knode, "READS", xnodes[-1], yomi="on"))

            if tk['reading'].has_key('ja_kun'):
                for tr in tk['reading']['ja_kun']:
                    xnodes.append(py2neo.Node("Reading", reading=tr))
                    xrels.append(py2neo.Relationship(knode, "READS", xnodes[-1], yomi="kun"))

            if tk['reading'].has_key('nanori'):
                for tr in tk['reading']['nanori']:
                    xnodes.append(py2neo.Node("Reading", reading=tr))
                    xrels.append(py2neo.Relationship(knode, "READS", xnodes[-1], yomi="nanori"))

        # Joyo
        if tk.has_key('grade') and tk.has_key('jindex'):
            xnodes.append(py2neo.Node("Joyo", joyo=int(tk['grade'])))
            xrels.append(py2neo.Relationship(xnodes[-1], "SUBSET", knode, jindex=tk['jindex']))

        # JLPT
        if tk.has_key('jlpt') and isinstance(tk['jlpt'],int):
            xnodes.append(py2neo.Node("Jlpt", jlpt=int(tk['jlpt'])))
            xrels.append(py2neo.Relationship(xnodes[-1], "SUBSET", knode))

        # SKIP
        if tk.has_key('qcode') and tk['qcode'].has_key('skip'):
            xnodes.append(py2neo.Node("Skip", skip=tk['qcode']['skip']))
            xrels.append(py2neo.Relationship(knode, "WRITTEN", xnodes[-1]))

        # Create Kanji node
        try:
            neo.create(knode)
        except Exception as e:
            logexc(e,u'Failed to create Kanji node')

        # Create nodes
        for tnode in xnodes:
            try:
                neo.create(tnode)
            except Exception as e:
                logexc(e,u'Failed to create aux node')

        # Build relations
        for trel in xrels:
            # Check if Nodes are bound
            sn = trel.start_node
            en = trel.end_node
            # if start node is not bound, then attempt a lookup
            if not sn.bound:
                nlab = list(sn.labels)[0]
                nsn = neo.find_one(nlab,nlab.lower(),sn[nlab.lower()])
                if nsn:
                    logthis(">>> Xref OK: %s '%s'" % (nlab,sn[nlab.lower()]),loglevel=LL.DEBUG)
                    sn = nsn
            # if end node is not bound, then attempt a lookup
            if not en.bound:
                elab = list(en.labels)[0]
                nen = neo.find_one(elab,elab.lower(),en[elab.lower()])
                if nen:
                    logthis(">>> Xref OK: %s '%s'" % (elab,en[elab.lower()]),loglevel=LL.DEBUG)
                    en = nen
            # Rebuild relationship
            rrel = py2neo.Relationship(sn,trel.type,en,**trel.properties)
            try:
                neo.create_unique(rrel)
            except Exception as e:
                logexc(e,"Failed to build relationship")


def get_topitem(inlist):
    """Get highest-ranked item from a dict of values"""
    zlist = sorted(inlist.items(), key=operator.itemgetter(1), reverse=1)
    if len(zlist) > 0:
        return zlist[0][0]
    else:
        return False
