#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# db - ed2/db.py
# edparse2: Database wrapper classes
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

import sys
import os
import re
import json
import time

import pymongo
import redis as xredis
import bson.regex as brgx
import psycopg2
import psycopg2.extras
import MeCab
import MySQLdb
import MySQLdb.cursors

# Logging & Error handling
from .common.logthis import *

class mongo:
    """Hotamod class for handling Mongo stuffs"""
    xcon = None
    xcur = None
    silence = False
    db = None

    def __init__(self, uri, silence=False):
        """Initialize and connect to MongoDB"""
        self.silence = silence

        try:
            self.xcon = pymongo.MongoClient(uri)
            self.db = uri.split('/')[-1]
        except Exception as e:
            logthis("Failed connecting to Mongo --",loglevel=LL.ERROR,suffix=e)
            return False

        self.xcur = self.xcon[self.db]
        if not self.silence: logthis("Connected to Mongo OK",loglevel=LL.INFO,ccode=C.GRN)

    def find(self, collection, query):
        xresult = {}
        xri = 0
        for tresult in self.xcur[collection].find(query):
            xresult[xri] = tresult
            xri += 1
        return xresult

    def update_set(self, collection, monid, setter):
        try:
            self.xcur[collection].update({'_id': monid}, {'$set': setter})
        except Exception as e:
            logthis("Failed to update document(s) in Mongo --",loglevel=LL.ERROR,suffix=e)

    def upsert(self, collection, monid, indata):
        try:
            return self.xcur[collection].update({'_id': monid}, indata, upsert=True)
        except Exception as e:
            logthis("Failed to upsert document in Mongo --",loglevel=LL.ERROR,suffix=e)
            return None

    def findOne(self, collection, query):
        return self.xcur[collection].find_one(query)

    def insert(self, collection, indata):
        return self.xcur[collection].insert_one(indata).inserted_id

    def insert_many(self, collection, indata):
        return self.xcur[collection].insert_many(indata).inserted_ids

    def count(self, collection):
        return self.xcur[collection].count()

    def getone(self, collection, start=0):
        for trez in self.xcur[collection].find().skip(start).limit(1):
            return trez

    def delete(self, collection, query):
        return self.xcur[collection].delete_one(query)

    def close(self):
        if self.xcon:
            self.xcon.close()
            if not self.silence: logthis("Disconnected from Mongo")

    def __del__(self):
        """Disconnect from MongoDB"""
        if self.xcon:
            self.xcon.close()
            #if not self.silence: logthis("Disconnected from Mongo")


class redis:
    """Hotamod class for Redis stuffs"""
    rcon = None
    rpipe = None
    conndata = {}
    rprefix = 'hota'
    silence = False

    def __init__(self, cdata={}, prefix='',silence=False):
        """Initialize Redis"""
        self.silence = silence
        if cdata:
            self.conndata = cdata
        if prefix:
            self.rprefix = prefix
        try:
            self.rcon = xredis.Redis(**self.conndata)
        except Exception as e:
            logthis("Error connecting to Redis",loglevel=LL.ERROR,suffix=e)
            return

        if not self.silence: logthis("Connected to Redis OK",loglevel=LL.INFO,ccode=C.GRN)


    def set(self, xkey, xval, usepipe=False, noprefix=False):
        if noprefix: zkey = xkey
        else:        zkey = '%s:%s' % (self.rprefix, xkey)
        if usepipe:
            xrez = self.rpipe.set(zkey, xval)
        else:
            xrez = self.rcon.set(zkey, xval)
        return xrez

    def setex(self, xkey, xval, expiry, usepipe=False, noprefix=False):
        if noprefix: zkey = xkey
        else:        zkey = '%s:%s' % (self.rprefix, xkey)
        if usepipe:
            xrez = self.rpipe.setex(zkey, xval, expiry)
        else:
            xrez = self.rcon.setex(zkey, xval, expiry)
        return xrez

    def get(self, xkey, usepipe=False, noprefix=False):
        if noprefix: zkey = xkey
        else:        zkey = '%s:%s' % (self.rprefix, xkey)
        if usepipe:
            xrez = self.rpipe.set(zkey)
        else:
            xrez = self.rcon.get(zkey)
        return xrez

    def incr(self, xkey, usepipe=False):
        if usepipe:
            xrez = self.rpipe.incr('%s:%s' % (self.rprefix, xkey))
        else:
            xrez = self.rcon.incr('%s:%s' % (self.rprefix, xkey))
        return xrez

    def exists(self, xkey, noprefix=False):
        return self.rcon.exists('%s:%s' % (self.rprefix, xkey))

    def keys(self, xkey, noprefix=False):
        if noprefix: zkey = xkey
        else:        zkey = '%s:%s' % (self.rprefix, xkey)
        return self.rcon.keys(zkey)

    def makepipe(self):
        try:
            self.rpipe = self.rcon.pipeline()
        except Exception as e:
            logthis("Error creating Redis pipeline",loglevel=LL.ERROR,suffix=e)

    def execpipe(self):
        if self.rpipe:
            self.rpipe.execute()
            logthis("Redis: No pipeline to execute",loglevel=LL.ERROR)

    def count(self):
        return self.rcon.dbsize()

    def lrange(self,qname,start,stop):
        return self.rcon.lrange(self.rprefix+":"+qname,start,stop)

    def llen(self,qname):
        return self.rcon.llen(self.rprefix+":"+qname)

    def lpop(self,qname):
        return self.rcon.lpop(self.rprefix+":"+qname)

    def lpush(self,qname,xval):
        return self.rcon.lpush(self.rprefix+":"+qname,xval)

    def rpop(self,qname):
        return self.rcon.rpop(self.rprefix+":"+qname)

    def rpush(self,qname,xval):
        return self.rcon.rpush(self.rprefix+":"+qname,xval)

    def blpop(self,qname,timeout=0):
        return self.rcon.blpop(self.rprefix+":"+qname,timeout)

    def brpop(self,qname,timeout=0):
        return self.rcon.brpop(self.rprefix+":"+qname,timeout)

    def brpoplpush(self,qsname,qdname,timeout=0):
        return self.rcon.brpoplpush(self.rprefix+":"+qsname,self.rprefix+":"+qdname,timeout)

    def __del__(self):
        pass
        #if not self.silence: logthis("Disconnected from Redis")


class mysql:
    xcon = None
    xcur = None
    conndata = {}
    silence = False

    def __init__(self, cdata={}, silence=False):
        """Initialize and connect to mySQL"""
        self.silence = silence
        if cdata:
            self.conndata = cdata
        # set DictCursor class
        self.conndata['cursorclass'] = MySQLdb.cursors.DictCursor
        # try connecting
        try:
            self.xcon = MySQLdb.connect(**self.conndata)
        except Exception as e:
            logthis("Failed connecting to mySQL --",LL.ERROR,suffix=e)
            return

        self.xcur = self.xcon.cursor()
        if not self.silence: logthis("Connected to mySQL OK",LL.INFO,ccode=C.GRN)

    def getbyid(self, table, xid):
        """Get row by ID"""
        qto = "SELECT * FROM %s WHERE id = %i" % (table, xid)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()[0]
        except Exception as e:
            logthis("mySQL query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def count(self, table):
        """Return count for a particular table"""
        qto = "SELECT count(*) FROM %s" % (table)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()[0]['count(*)']
        except Exception as e:
            logthis("mySQL query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def close(self):
        """Disconnect from mySQL"""
        if self.xcon:
            self.xcon.close()
            if not self.silence: logthis("Disconnected from mySQL")

    def __del__(self):
        if self.xcon:
            self.xcon.close()
            if not self.silence: logthis("Disconnected from mySQL")


class psql:
    """Hotamod class for handling Postgres stuffs"""
    xcon = None
    xcur = None
    conndata = {}
    silence = False


    def __init__(self, cdata={}, peer_auth=False, silence=False):
        """Initialize and connect to Postgres"""
        self.silence = silence
        if cdata:
            self.conndata = cdata

        # if peer_auth = True, then connect using Peer Authentication rather than Password auth
        # This authenticates the user based on the effective UID of the running process
        if peer_auth:
            connstr = "dbname=%s" % (self.conndata['database'])
        else:
            connstr = "host=%s dbname=%s user=%s password=%s" % (self.conndata['host'], self.conndata['database'], self.conndata['user'], self.conndata['pass'])

        try:
            self.xcon = psycopg2.connect(connstr)
        except Exception as e:
            logthis("Error connecting to Postgres --",LL.ERROR,suffix=e)
            return

        self.xcur = self.xcon.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if not self.silence: logthis("Connected to Postgres OK",LL.INFO,ccode=C.GRN)

    def query(self, query):
        """Sent query to Postgres; return the results"""
        try:
            xcur.execute(query) # TODO JACOB
            qout = self.xcur.fetchall()
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def find(self, table, tval, query):
        """Perform a simple query on a specified column for a LIKE match"""
        qto = "SELECT * FROM %s WHERE %s LIKE '%%%s%%'" % (table, tval, query)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def xfind(self, table, tval, query):
        """Perform a simple query on a specified column with an exact match"""
        qto = "SELECT * FROM %s WHERE %s = '%s'" % (table, tval, query)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def getbyid(self, table, xid):
        """Get row by ID"""
        qto = "SELECT * FROM %s WHERE id = %i" % (table, xid)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()[0]
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def getset(self, table, offset, count):
        """Retrieve set number of rows, starting at an offset"""
        qto = "SELECT * FROM %s OFFSET %i LIMIT %i" % (table, offset, count)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def getrange(self, table, rstart, rsize):
        """Retrieve range of rows with a start ID and stop ID"""
        qto = "SELECT * FROM %s WHERE id >= %i AND id < %i" % (table, rstart, rstart + rsize)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def count(self, table):
        """Return count for a particular table"""
        qto = "SELECT count(*) FROM %s" % (table)
        try:
            self.xcur.execute(qto)
            qout = self.xcur.fetchall()[0]['count']
        except Exception as e:
            logthis("Postgres query failed --",LL.ERROR,suffix=e)
            qout = None
        return qout

    def close(self):
        if self.xcon:
            self.xcon.close()
            if not self.silence: logthis("Disconnected from Postgres")

    def __del__(self):
        """Close connection DB upon object deletion"""
        if self.xcon:
            self.xcon.close()
            if not self.silence: logthis("Disconnected from Postgres")

