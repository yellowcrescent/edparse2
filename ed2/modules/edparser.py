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
import json
import codecs
import copy

import lxml.etree as etree

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *

# input file target list
targets = ["jmdict","jmnedict","kradfile","kradfile2","kanjidic"]

# priority index lookup table
priodex = {
            'ichi1': 40, 'ichi2': 30, 'news1': 20, 'news2': 15, 'spec1': 30,
            'spec2': 15, 'gai1': 60, 'gai2': 45, 'nf01': 500, 'nf02': 495,
            'nf03': 495, 'nf04': 495, 'nf05': 490, 'nf06': 490, 'nf07': 480,
            'nf08': 480, 'nf09': 470, 'nf10': 470, 'nf11': 450, 'nf12': 440,
            'nf13': 430, 'nf14': 420, 'nf15': 410, 'nf16': 400, 'nf17': 390,
            'nf18': 380, 'nf19': 370, 'nf20': 350, 'nf21': 340, 'nf22': 330,
            'nf23': 320, 'nf24': 310, 'nf25': 300, 'nf26': 290, 'nf27': 280,
            'nf28': 270, 'nf29': 260, 'nf30': 250, 'nf31': 240, 'nf32': 230,
            'nf33': 220, 'nf34': 210, 'nf35': 200, 'nf36': 180, 'nf37': 170,
            'nf38': 165, 'nf39': 145, 'nf40': 120, 'nf41': 100, 'nf42': 95,
            'nf43': 90, 'nf44': 75, 'nf45': 60, 'nf46': 45, 'nf47': 30, 'nf48': 15
          }

krdex  = {}

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

    # parse kanjidic
    kdex = parse_kanjidic(tmap['kanjidic'])

    # parse jmdict
    jmdict = parse_jmdict(tmap['jmdict'])

    # parse jmnedict
    nedict = parse_jmdict(tmap['jmnedict'])

    ## write output
    if xconfig.run.json:
        # Dump output to JSON file if --json/-j option is used
        logthis(">> Dumping output as JSON to",suffix=xconfig.run.json,loglevel=LL.INFO)
        try:
            with codecs.open(xconfig.run.json,"w","utf-8") as f:
                json.dump({ 'kanji': kdex, 'jmdict': jmdict, 'nedict': nedict }, f, indent=4, separators=(',', ': '))
        except Exception as e:
            logexc(e,"Failed to dump output to JSON file")
            failwith(ER.PROCFAIL,"File operation failed. Aborting.")
    else:
        # MongoDB
        update_mongo(xconfig.mongo.uri,kdex,jmdict,nedict)

    return 0


def update_mongo(mongo_uri,kdex,jmdict,nedict):
    """
    Insert, upsert, or update entries in MongoDB
    """
    # connect to mongo
    logthis("Connecting to",suffix=mongo_uri,loglevel=LL.INFO)
    mdx = mongo(mongo_uri)

    # Kanji
    update_set(mdx, kdex, 'kanji')

    # JMDict
    update_set(mdx, jmdict, 'jmdict')

    # JMnedict
    update_set(mdx, nedict, 'jmnedict')


def update_set(mdx,indata,setname):
    """
    merge each existing entry with new entry
    """
    updated = 0
    created = 0

    logthis(">> Updating collection:",suffix=setname,loglevel=LL.INFO)

    for tk,tv in indata.iteritems():
        xkan = mdx.findOne(setname, { '_id': tk })
        if xkan:
            # modify existing object with new data
            iobj = xkan
            iobj.update(tv)
        else:
            iobj = tv

        if mdx.upsert(setname, tk, iobj)['updatedExisting']:
            updated += 1
        else:
            created += 1

    logthis("update complete - updated: %d / created: %d / total:" % (updated,created),prefix=setname,suffix=(updated+created),loglevel=LL.INFO)


def parse_kradfile(krfile,encoding='euc-jp'):
    """
    Parse KRADFILE & KRADFILE2
    These are typically EUC-JP (JIS X 0212) encoded files, but a different
    encoding can be specified with the 'encoding' parameter
    """
    global krdex
    logthis("Parsing file",suffix=krfile,loglevel=LL.INFO)

    # convert file from EUC-JP to Unicode
    with codecs.open(krfile,'r',encoding) as f:
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


def parse_kanjidic(kdfile):
    """
    Parse KanjiDic2 XML file
    """
    global krdex
    logthis("Parsing KanjiDic2 XML file",suffix=kdfile,loglevel=LL.INFO)

    # parse XML as a stream using lxml etree parser
    elist = {}
    curEntry = {}
    entries = 0
    for event,elem in etree.iterparse(kdfile, events=('end', 'start-ns')):
        if event == "end" and elem.tag == "character":
            # kanji
            curEntry['kanji'] = elem.find('literal').text

            # code points
            curEntry['codepoint'] = {}
            for sv in elem.find('codepoint').findall('cp_value'):
                curEntry['codepoint'][sv.attrib['cp_type']] = sv.text

            # radicals
            curEntry['radical'] = {}
            for sv in elem.find('radical').findall('rad_value'):
                curEntry['radical'][sv.attrib['rad_type']] = sv.text

            ## misc
            misc = elem.find('misc')

            # grade/joyo level
            # NEW: now returns an integer; for hyougaji kanji, this will be zero
            if misc.find('grade') is not None:
                curEntry['grade'] = int(misc.find('grade').text)
            else:
                curEntry['grade'] = 0

            # stroke_count
            # NEW: this will now *always* be an array of ints, whereas before it was
            # a regular string *most* of the time, and an array of strings *sometimes*
            curEntry['stroke_count'] = []
            for sv in misc.findall('stroke_count'):
                curEntry['stroke_count'].append(int(sv.text))

            # freq
            # NEW: this is now converted to an int;
            # if it does not exist, it is null instead of an empty string
            if misc.find('freq') is not None:
                curEntry['freq'] = int(misc.find('freq').text)
            else:
                curEntry['freq'] = None

            # jlpt
            # NEW: if it does not exist, it is null instead of an empty string
            if misc.find('jlpt') is not None:
                curEntry['jlpt'] = int(misc.find('jlpt').text)
            else:
                curEntry['jlpt'] = None

            # variant
            # NEW: this field was not previously parsed by edparse
            curEntry['variant'] = {}
            for sv in misc.findall('variant'):
                if curEntry['variant'].has_key(sv.attrib['var_type']):
                    curEntry['variant'][sv.attrib['var_type']].append(sv.text)
                else:
                    curEntry['variant'][sv.attrib['var_type']] = [sv.text]

            # xref
            curEntry['xref'] = {}
            if elem.find('dic_number') is not None:
                for sv in elem.find('dic_number').findall('dic_ref'):
                    if sv.attrib['dr_type'] == "moro":
                        curEntry['xref'][sv.attrib['dr_type']] = "%02d:%04d/%s" % (int(sv.attrib.get('m_vol',0)), int(sv.attrib.get('m_page',0)), sv.text)
                    else:
                        curEntry['xref'][sv.attrib['dr_type']] = sv.text

            # qcode
            # NEW: now handles skip_misclass; types with multiple entries are coerced to lists
            curEntry['qcode'] = {}
            if elem.find('query_code') is not None:
                for sv in elem.find('query_code').findall('q_code'):
                    if sv.attrib.has_key('skip_misclass'):
                        if not curEntry['qcode'].has_key('skip_misclass'):
                            curEntry['qcode']['skip_misclass'] = []
                        curEntry['qcode']['skip_misclass'].append({ 'misclass': sv.attrib['skip_misclass'], 'skip': sv.text })
                    else:
                        if curEntry['qcode'].has_key(sv.attrib['qc_type']):
                            # convert to list if we encounter another entry
                            if curEntry['qcode'][sv.attrib['qc_type']] is not list:
                                curEntry['qcode'][sv.attrib['qc_type']] = [curEntry['qcode'][sv.attrib['qc_type']]]
                            curEntry['qcode'][sv.attrib['qc_type']].append(sv.text)
                        else:
                            curEntry['qcode'][sv.attrib['qc_type']] = sv.text

            ## reading & meaning & nanori
            curEntry['reading'] = {}
            curEntry['meaning'] = {}
            if elem.find('reading_meaning') is not None:
                # nanori
                curEntry['reading']['nanori'] = []
                for sv in elem.find('reading_meaning').findall('nanori'):
                    curEntry['reading']['nanori'].append(sv.text)

                if elem.find('reading_meaning').find('rmgroup') is not None:
                    # reading
                    for sv in elem.find('reading_meaning').find('rmgroup').findall('reading'):
                        if not curEntry['reading'].has_key(sv.attrib['r_type']):
                            curEntry['reading'][sv.attrib['r_type']] = []
                        curEntry['reading'][sv.attrib['r_type']].append(sv.text)

                    # meaning
                    for sv in elem.find('reading_meaning').find('rmgroup').findall('meaning'):
                        if sv.attrib.has_key('m_lang'):
                            mlang = sv.attrib['m_lang']
                        else:
                            mlang = 'en'
                        if not curEntry['meaning'].has_key(mlang):
                            curEntry['meaning'][mlang] = []
                        curEntry['meaning'][mlang].append(sv.text)

            # krad: crossref radicals
            if krdex.has_key(curEntry['kanji']):
                curEntry['krad'] = krdex[curEntry['kanji']]

            # set _id for Mongo
            curEntry['_id'] = curEntry['codepoint']['ucs']


            elist[curEntry['_id']] = copy.deepcopy(curEntry)
            logthis("Commited entry:\n",suffix=print_r(curEntry),loglevel=LL.DEBUG)
            curEntry.clear()
            elem.clear()
            entries += 1

    logthis("** Kanji parsed:",suffix=entries,loglevel=LL.INFO)
    return elist


def parse_jmdict(kdfile,seqbase=3000000):
    """
    Parse JMDict/JMnedict XML files
    """
    global krdex
    logthis("Parsing JMDict/JMnedict XML file",suffix=kdfile,loglevel=LL.INFO)

    # parse XML as a stream using lxml etree parser
    elist = {}
    curEntry = {}
    entries = 0
    entList = {}
    revEntList = {}
    for event,elem in etree.iterparse(kdfile, events=('end', 'start-ns')):
        if event == "end" and elem.tag == "entry":
            # resolve entities
            if not entList:
                entList,revEntList = resolveEntities(elem.getroottree().docinfo.internalDTD.entities())

            # ent_seq
            curEntry['ent_seq'] = elem.find('ent_seq').text

            # set _id
            curEntry['_id'] = curEntry['ent_seq']

            ## k_ele
            kf_pmax = 0
            if elem.find('k_ele') is not None:
                curEntry['k_ele'] = []
                for sv in elem.findall('k_ele'):
                    kele = {}

                    # k_ele.keb
                    kele['keb'] = sv.find('keb').text

                    # k_ele.ke_inf
                    for ssv in sv.findall('ke_inf'):
                        if not kele.has_key('ke_inf'):
                            kele['ke_inf'] = {}
                        kele['ke_inf'][revEntList[ssv.text]] = ssv.text

                    # k_ele.ke_pri
                    for ssv in sv.findall('ke_pri'):
                        if not kele.has_key('ke_pri'):
                            kele['ke_pri'] = []
                        kele['ke_pri'].append(ssv.text)
                        kf_pmax += priodex[ssv.text]

                    curEntry['k_ele'].append(kele)

            curEntry['kf_pmax'] = kf_pmax

            ## r_ele
            rf_pmax = 0
            if elem.find('r_ele') is not None:
                curEntry['r_ele'] = []
                for sv in elem.findall('r_ele'):
                    rele = {}

                    # r_ele.reb
                    rele['reb'] = sv.find('reb').text

                    # r_ele.re_nokanji
                    if sv.find('re_nokanji') is not None:
                        rele['re_nokanji'] = True

                    # r_ele.restr
                    for ssv in sv.findall('re_restr'):
                        if not rele.has_key('re_restr'):
                            rele['re_restr'] = []
                        rele['re_restr'].append(ssv.text)

                    # r_ele.re_inf
                    for ssv in sv.findall('re_inf'):
                        if not rele.has_key('re_inf'):
                            rele['re_inf'] = {}
                        rele['re_inf'][revEntList[ssv.text]] = ssv.text

                    # r_ele.re_pri
                    for ssv in sv.findall('re_pri'):
                        if not rele.has_key('re_pri'):
                            rele['re_pri'] = []
                        rele['re_pri'].append(ssv.text)
                        rf_pmax += priodex[ssv.text]

                    curEntry['r_ele'].append(rele)

            curEntry['rf_pmax'] = rf_pmax

            ## sense (JMDict)
            if elem.find('sense') is not None:
                curEntry['sense'] = []
                for sv in elem.findall('sense'):
                    sen = {}

                    # sense.stagk
                    for ssv in sv.findall('stagk'):
                        if not sen.has_key('stagk'):
                            sen['stagk'] = []
                        sen['stagk'].append(ssv.text)

                    # sense.stagr
                    for ssv in sv.findall('stagr'):
                        if not sen.has_key('stagr'):
                            sen['stagr'] = []
                        sen['stagr'].append(ssv.text)

                    # sense.xref
                    for ssv in sv.findall('xref'):
                        if not sen.has_key('xref'):
                            sen['xref'] = []
                        sen['xref'].append(ssv.text)

                    # sense.ant
                    for ssv in sv.findall('ant'):
                        if not sen.has_key('ant'):
                            sen['ant'] = []
                        sen['ant'].append(ssv.text)

                    # sense.ant
                    for ssv in sv.findall('ant'):
                        if not sen.has_key('ant'):
                            sen['ant'] = []
                        sen['ant'].append(ssv.text)

                    # sense.pos
                    for ssv in sv.findall('pos'):
                        if not sen.has_key('pos'):
                            sen['pos'] = {}
                        sen['pos'][revEntList[ssv.text]] = ssv.text

                    # sense.field
                    for ssv in sv.findall('field'):
                        if not sen.has_key('field'):
                            sen['field'] = {}
                        sen['field'][revEntList[ssv.text]] = ssv.text

                    # sense.misc
                    for ssv in sv.findall('misc'):
                        if not sen.has_key('misc'):
                            sen['misc'] = {}
                        sen['misc'][revEntList[ssv.text]] = ssv.text

                    # sense.lsource
                    for ssv in sv.findall('lsource'):
                        if not sen.has_key('lsource'):
                            sen['lsource'] = []
                        sen['lsource'].append(ssv.text)

                    # sense.dial
                    for ssv in sv.findall('dial'):
                        if not sen.has_key('dial'):
                            sen['dial'] = []
                        sen['dial'].append(ssv.text)

                    # sense.gloss
                    if sv.find('gloss') is not None:
                        sen['gloss'] = {}
                        for ssv in sv.findall('gloss'):
                            if len(ssv.attrib):
                                mlang = ssv.attrib.values()[0]
                            else:
                                mlang = "eng"
                            if not sen['gloss'].has_key(mlang):
                                sen['gloss'][mlang] = []
                            sen['gloss'][mlang].append(ssv.text)

                    # sense.example
                    for ssv in sv.findall('example'):
                        if not sen.has_key('example'):
                            sen['example'] = []
                        sen['example'].append(ssv.text)

                    # sense.s_inf
                    for ssv in sv.findall('s_inf'):
                        if not sen.has_key('s_inf'):
                            sen['s_inf'] = []
                        sen['s_inf'].append(ssv.text)

                    # sense.pri
                    for ssv in sv.findall('pri'):
                        if not sen.has_key('pri'):
                            sen['pri'] = []
                        sen['pri'].append(ssv.text)

                    curEntry['sense'].append(sen)

            ## trans (JMnedict)
            if elem.find('trans') is not None:
                curEntry['trans'] = []
                for sv in elem.findall('trans'):
                    tran = {}

                    # trans.name_type
                    for ssv in sv.findall('name_type'):
                        if not tran.has_key('name_type'):
                            tran['name_type'] = []
                        tran['name_type'].append(ssv.text)

                    # trans.xref
                    for ssv in sv.findall('xref'):
                        if not tran.has_key('xref'):
                            tran['xref'] = []
                        tran['xref'].append(ssv.text)

                    # trans.trans_det
                    if sv.find('trans_det') is not None:
                        tran['trans_det'] = {}
                        for ssv in sv.findall('trans_det'):
                            if len(ssv.attrib):
                                mlang = ssv.attrib.values()[0]
                            else:
                                mlang = "eng"
                            if not tran['trans_det'].has_key(mlang):
                                tran['trans_det'][mlang] = []
                            tran['trans_det'][mlang].append(ssv.text)


            elist[curEntry['_id']] = copy.deepcopy(curEntry)
            logthis("Commited entry:\n",suffix=print_r(curEntry),loglevel=LL.DEBUG)
            curEntry.clear()
            elem.clear()
            entries += 1

    logthis("** Entries parsed:",suffix=entries,loglevel=LL.INFO)
    return elist


def resolveEntities(entlist):
    """
    build two dicts of entities; a forward and reverse mapping
    Entity list can be accessed via Element.getroottree().docinfo.internalDTD.entities(),
    which returns a list of lxml.etree._DTDEntityDecl objects, which this func accepts
    """
    fmap = {}
    rmap = {}
    for te in entlist:
        fmap[te.name] = te.content
        rmap[te.content] = te.name

    return fmap,rmap
