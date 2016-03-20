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

from ed2.common.logthis import *
from ed2.common.util import *
from ed2.db import *

