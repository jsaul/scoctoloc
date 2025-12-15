###########################################################################
# Copyright (C) GFZ Potsdam                                               #
# All rights reserved.                                                    #
#                                                                         #
# Author: Joachim Saul (saul@gfz.de)                                      #
#                                                                         #
# GNU Affero General Public License Usage                                 #
# This file may be used under the terms of the GNU Affero                 #
# Public License version 3.0 as published by the Free Software Foundation #
# and appearing in the file LICENSE included in the packaging of this     #
# file. Please review the following information to ensure the GNU Affero  #
# Public License version 3.0 requirements will be met:                    #
# https://www.gnu.org/licenses/agpl-3.0.html.                             #
###########################################################################

import scocto.util
import fnmatch

class StreamWhitelist(list):
    """
    Specify stream whitelist as list of items in the format
    net.sta.loc.cha

    with net, sta, loc, cha being the network, station, location and
    channel code, respectively. Not all neet to be specified, missing
    fields are treated as wildcard.

    For instance:

        IU

    matches all stations and channels in the IU network.

        IU
        GE.FALKS

    matches all stations and channels in the IU network plus station
    GE.FALKS. Comments may be added or entries may be commented out:

        # IU
        # comment
        GE.FALKS

    We can also write it all on one line:

        IU GE.FALKS

    You get the picture.
    """

    @staticmethod
    def FromFile(filename):
        return StreamWhitelist(filename=filename)

    @staticmethod
    def FromText(text):
        return StreamWhitelist(text=text)

    def __init__(self, text=None, filename=None):
        if text:
            self.parse(text)
        elif filename:
            text = open(filename).read().strip()
            self.parse(text)

    def parse(self, text):
        self.clear()
        items = list()
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#"): 
                continue
            for item in line.split():
                items.append(item)
        for item in items:
            item = [ t.strip() for t in item.split(".") ]
            item.extend(["*", "*", "*"])
            item = item[:4]
            if item[-2] == "":
                item[-2] = "--"
            self.append(".".join(item))
        pass

    def matches(self, stream_id):
        n, s, l, c = scocto.util.nslc(stream_id)
        if l == "":
            l = "--"
        stream_id = "%s.%s.%s.%s" % (n, s, l, c)
        for glob in self:
            if fnmatch.fnmatchcase(stream_id, glob):
                return True
