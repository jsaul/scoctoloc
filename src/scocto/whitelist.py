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

import scstuff.util
import fnmatch

class StreamWhitelist(list):

    def __init__(self, filename=None):
        if filename:
            self.read(filename)

    def read(self, filename):
        with open(filename) as f:
            self.clear()
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith("#"): 
                    continue
                line = [ t.strip() for t in line.split(".") ]
                line.extend(["*", "*", "*"])
                line = line[:4]
                if line[-2] == "":
                    line[-2] = "--"
                self.append(".".join(line))

    def matches(self, stream_id):
        n, s, l, c = scstuff.util.nslc(stream_id)
        if l == "":
            l = "--"
        stream_id = "%s.%s.%s.%s" % (n, s, l, c)
        for glob in self:
            if fnmatch.fnmatchcase(stream_id, glob):
                return True
