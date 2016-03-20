#!/usr/bin/env python
# coding=utf-8
###############################################################################
#
# mecab - ed2/mecab.py
# edparse2: MeCab wrapper class
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

import MeCab

from .common.logthis import *


class hjparse:
    tagger = None
    mcparams = "-Owakati"

    # conjugate mapping
    cmap = {
        '特殊':       "irregular",
        'ナイ':       "nai_suffix",
        '五段':       "godan_verb",
        'ワ行促音便': "u_subtype_v5u",
        'ラ行特殊':   "ru_subtype_v5r-i",
        '一段':       "ichidan_verb_v1",
        '形容詞':     "i_adj",
        'アウオ段':   "auo_subtype",
        'イ段':       "i_subtype"
    }
    cmap_full = {
        '特殊':       "irregular",
        'ナイ':        "(-nai) suffix",
        '五段':       "godan verb(I)[v5]",
        'ワ行促音便': "(-u) subtype, nasal geminate [v5u]",
        'ラ行特殊':   "(-ru) subtype, irregular [v5r-i]",
        '一段':       "ichidan verb (II)[v1]",
        '形容詞':     "(-i) adjective",
        'アウオ段':   "(-a,-u,-o) subtype",
        'イ段':       "(-i) subtype"
    }

    # inflection mapping
    imap = {
        '基本形':      "base",
        '連用形':      "conjunctive",
        '未然形':      "imperfective",
        '命令ｉ':      "imperative"
    }
    imap_full = {
        '基本形':      "base",
        '連用形':      "conjunctive (-masu stem)",
        '未然形':      "imperfective",
        '命令ｉ':      "imperative (i)"
    }

    # part-of-speech and subcategory mapping
    pmap = {
        '連体詞':      "adj_adnominal",
        '接頭詞':      "prefix",
        '形容詞接続':  "prefix_adj",
        '数接続':      "prefix_number",
        '動詞接続':    "prefix_verb",
        '名詞接続':    "prefix_noun",
        '名詞':        "noun",
        '引用文字列':  "quoted_str",
        'サ変接続':    "suru_verb-noun_stem",
        'ナイ形容詞語幹': "nai_adj-noun_stem",
        '形容動詞語幹': "na_adj-verb_stem",
        '動詞非自立的': "verb-dependent_noun",
        '副詞可能':    "adverb_possible",
        '一般':       "general",
        '数':         "number",
        '接続詞的':   "conjunction",
        '固有名詞':   "proper_noun",
        '人名':       "personal",
        '姓':         "surname_name",
        '名':         "given_name",
        '組織':       "org_name",
        '地域':       "area_region",
        '国':         "country",
        '接尾':       "suffix",
        '助数詞':     "suffix_counter",
        '助動詞語幹':  "verb_aux_stem",
        '特殊':       "special",
        '代名詞':     "pronoun",
        '縮約':       "contraction",
        '非自立':     "dependent",
        '動詞':       "verb",
        '自立':       "independent",
        '形容詞':     "adjective",
        '副詞':       "adverb",
        '助詞類接続': "adverb_conjunctive",
        '接続詞':     "conjunction",
        '助詞':       "particle",
        '格助詞':     "particle_case",
        '引用':       "particle_citation",
        '連語':       "particle_collocation_association",
        '係助詞':     "particle_subject-binding",
        '終助詞':     "particle_final",
        '接続助詞':   "particle_conjunctive",
        '副詞化':     "particle_adverb",
        '副助詞':     "particle_adverbial",
        '並立助詞':   "particle_parallel",
        '連体化':     "particle_adnominal",
        '助動詞':     "verb_aux",
        '感動詞':     "interjection",
        '記号':       "symbol",
        '句点':       "period",
        '読点':       "comma",
        '空白':       "space",
        'アルファベット': "alphabet",
        '括弧開':     "parenthesis_open",
        '括弧閉':     "parenthesis_close",
        'フィラー':    "filler",
        'その他':     "other",
        '間投':       "interjection-delay_sounds",
        '未知語':     "unknown",
        'BOS/EOS':   "end-begin_sentence",
        '*': "-"
    }
    pmap_full = {
        '連体詞':      "adnominal adjective",
        '接頭詞':      "prefix",
        '形容詞接続':   "adjective prefix",
        '数接続':      "number prefix",
        '動詞接続':    "verb prefix",
        '名詞接続':    "noun prefix",
        '名詞':       "noun",
        '引用文字列':  "quoted string",
        'サ変接続':    "-suru verb/noun stem",
        'ナイ形容詞語幹': "-nai adj/noun stem",
        '形容動詞語幹': "-na adj/verb stem",
        '動詞非自立的': "verb-dependent noun",
        '副詞可能':    "adverb possible",
        '一般':       "general/common",
        '数':         "number",
        '接続詞的':    "conjunction",
        '固有名詞':    "proper noun",
        '人名':       "personal",
        '姓':         "surname/family name",
        '名':         "first/given name",
        '組織':       "organization name",
        '地域':       "area/region",
        '国':         "country",
        '接尾':       "suffix",
        '助数詞':     "counter/unit suffix",
        '助動詞語幹':  "aux verb stem",
        '特殊':       "special",
        '代名詞':     "pronoun",
        '縮約':       "contraction",
        '非自立':     "dependent",
        '動詞':       "verb",
        '自立':       "independent/base",
        '形容詞':     "adjective",
        '副詞':       "adverb",
        '助詞類接続':  "conjunctive adverb",
        '接続詞':     "conjunction",
        '助詞':       "particle",
        '格助詞':     "case particle",
        '引用':       "citation particle",
        '連語':       "collocation/association",
        '係助詞':     "subject/binding particle",
        '終助詞':     "final particle",
        '接続助詞':   "conjunctive particle",
        '副詞化':     "adverb particle",
        '副助詞':     "adverbial particle",
        '並立助詞':   "parallel particle",
        '連体化':     "adnominal particle",
        '助動詞':     "auxverb",
        '感動詞':     "interjection",
        '記号':       "symbol",
        '句点':       "period/fullstop",
        '読点':       "comma",
        '空白':       "space",
        'アルファベット': "alphabet",
        '括弧開':     "opening parenthesis/quote",
        '括弧閉':     "closing parenthesis/quote",
        'フィラー':    "filler",
        'その他':     "other",
        '間投':       "interjection/delay sounds",
        '未知語':     "unknown",
        'BOS/EOS':   "end/begin of sentence",
        '*': "-"
    }

    def __init__(self, mecab_params=""):
        if mecab_params:
            self.mcparams = mecab_params
        try:
            self.tagger = MeCab.Tagger(self.mcparams)
        except Exception as e:
            logexc(e, "Failed to initialize MeCab")

    def parseWakati(self, instr):
        """Outputs a wakati-formatted string (separates nodes by spaces)"""
        try:
            wakaout = self.tagger.parse(instr)
        except Exception as e:
            logexc(e, "parseWakati / MeCab.parse failed")
            wakaout = None
        return wakaout

    def parse(self, instr, noMarkers=False):
        xnodes = []

        try:
            node = self.tagger.parseToNode(instr).next
        except:
            logthis("MeCab node missing node.next",LL.WARNING)
            return []

        nomark_rgx = re.compile("^(unknown|end|parenthesis|period|symbol|number)",re.I)
        while node.surface:
            # get features
            tnode = self.getFeatures(node.feature)

            # set surface / token
            tnode['surface'] = node.surface

            if tnode['base'] != '*' and tnode['base'] != '-':
                if noMarkers:
                    if not nomark_rgx.match(tnode['pos']):
                        xnodes.append(tnode)
                else:
                    xnodes.append(tnode)

            # on to the next node...
            try:
                node = node.next
            except:
                logexc(e, "MeCab node missing node.next")
                break
        return xnodes

    def getFeatures(self, flist):
        """Parse feature string into a dict"""
        rawlist = flist.split(",")
        # POS main, Subtype 1, 2, 3, Conjugation (Conjunctive Type), Inflection (Conjunctive Form),
        # Base Form, Reading, Pronunciation
        if len(rawlist) < 7:
            logthis("MeCab returned a short feature list",LL.ERROR)
            return None

        xfeats = {
                    'pos': rawlist[0],
                    'subtype1': rawlist[1],
                    'subtype2': rawlist[2],
                    'subtype3': rawlist[3],
                    'conj': rawlist[4],
                    'infl': rawlist[5],
                    'base': rawlist[6]
                 }

        # set reading and pron
        if len(rawlist) == 9:
            xfeats['reading'] = rawlist[7]
            xfeats['pron'] = rawlist[8]
        else:
            xfeats['reading'] = '-'
            xfeats['pron'] = '-'

        # Part-of-speech
        try: xfeats['pos'] = self.pmap[rawlist[0]]
        except: xfeats['pos'] = rawlist[0]
        # POS - Subtype 1
        try: xfeats['subtype1'] = self.pmap[rawlist[1]]
        except: xfeats['subtype1'] = rawlist[1]
        # POS - Subtype 2
        try: xfeats['subtype2'] = self.pmap[rawlist[2]]
        except: xfeats['subtype2'] = rawlist[2]
        # POS - Subtype 3
        try: xfeats['subtype3'] = self.pmap[rawlist[3]]
        except: xfeats['subtype3'] = rawlist[3]
        # Conjugate
        try: xfeats['conj'] = self.cmap[rawlist[4]]
        except: xfeats['conj'] = rawlist[4]
        # Inflection
        try: xfeats['infl'] = self.imap[rawlist[5]]
        except: xfeats['infl'] = rawlist[5]

        return xfeats
