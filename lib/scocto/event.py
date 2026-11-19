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

import seiscomp.logging
import scocto.util


class MyEvent:
    # Used here locally to associate origins of the same event
    # and to remember the publication history.
    def __init__(self):
        # List of origins ordered by creation time
        self.origins = dict()

        # Dict of all picks referenced by this event
        self.picks = dict()

        # The currently preferred origin; usually self.origins[-1]
        self.preferredOrigin = None

        self.lastPublished = False

    def set_origin(self, origin, picks):
        method = origin.methodID()
        if method not in self.origins:
            self.origins[method] = list()
        self.origins[method].append(origin)
        for i in range(origin.arrivalCount()):
            arr = origin.arrival(i)
            self.picks[arr.pickID()] = picks[arr.pickID()]


class MyEventList(list):

    def find_matching_event(self, origin):

        method = origin.methodID()

        pick_ids = list()
        for i in range(origin.arrivalCount()):
            arr = origin.arrival(i)
            pick_ids.append(arr.pickID())

        matching_events = list()
        for event in self:
            if method not in event.origins:
                continue

            last = event.origins[method][-1]
            if scocto.util.originTimeSeparation(last, origin) < 30 and \
               scocto.util.originDistanceKm(last, origin) < 100:

                common_pick_count = 0

                for pick_id in pick_ids:
                    if pick_id in event.picks:
                        common_pick_count += 1

                if common_pick_count:
                    matching_events.append( (common_pick_count, event) )

        if matching_events:
            common_pick_count, matching_event = sorted(matching_events)[-1]
            seiscomp.logging.debug("Number of matching events: %d" % (len(matching_events)))
            for common_pick_count, event in matching_events:
                seiscomp.logging.debug("Common pick count: %d" % common_pick_count)
            return matching_event
