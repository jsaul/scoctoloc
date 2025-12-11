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

import seiscomp.datamodel
import seiscomp.io
import scstuff.util
import math
import numpy


def time2str(time):
    """
    Convert a seiscomp.core.Time to a string
    """
    return time.toString("%Y-%m-%d %H:%M:%S.%f000000")[:21]


def lat2str(lat):
    s = "%.3f " % abs(lat)
    if lat >= 0:
        s += "N"
    else:
        s += "S"
    return s


def lon2str(lon):
    s = "%.3f " % abs(lon)
    if lon >= 0:
        s += "E"
    else:
        s += "W"
    return s


def creationTime(obj):
    """ For readability """
    return obj.creationInfo().creationTime()


def pickTime(pick):
    """ For readability """
    return pick.time().value()


def sortedArrivals(origin):
    arrivals = [ origin.arrival(i) for i in range(origin.arrivalCount()) ]
    return sorted(arrivals, key=lambda t: t.distance())


def printOrigin(origin, picks):
    """
    Pretty printing an origin for debug output
    """ 
    lines = list()
    tim = origin.time().value()
    lat = origin.latitude().value()
    lon = origin.longitude().value()
    dep = origin.depth().value()
    lines.append("public id  %s" % origin.publicID())
    try:
        author = origin.creationInfo().author()
        if author:
            lines.append("author     %s" % origin.creationInfo().author())
    except ValueError:
        pass
    try:
        lines.append("method id  %s" % origin.methodID())
    except ValueError:
        pass
    lines.append("time       %s" % time2str(tim))
    lines.append("latitude   %s" % lat2str(lat))
    lines.append("longitude  %s" % lon2str(lon))
    lines.append("depth      %6.2f km" % dep)
    lines.append("arrivals:")

    picks = { p.publicID(): p for p in picks }

    for arr in sortedArrivals(origin):
        pick = picks[arr.pickID()]
        n, s, l, c = scstuff.util.nslc(pick.waveformID())
        p = arr.phase().code()
        d = arr.distance()
        a = arr.azimuth()
        r = arr.timeResidual()
        if l == "": 
            l = "--"
        line = "%-2s %6.3f %3.0f  %-2s %-5s %-2s  %s  %6.2f" % (p, d, a, n, s, l, time2str(pick.time().value()), r)
        lines.append(line)

    return "\n".join(lines)


def originReferencesPick(origin, pick):
    """
    Check whether one of the arrivals of the origin references the pick,
    in other words: whether the pick is associated to that origin.
    """
    pick_id = pick.publicID()
    for i in range(origin.arrivalCount()):
        arrival = origin.arrival(i)
        if arrival.pickID() == pick_id:
            return True
    return False


def sumOfLargestGaps(azi, n=2):
    """
    From an unsorted list of azimuth values, determine the
    largest n gaps and return their sum.
    """

    gap = []
    aziCount = len(azi)
    if aziCount < 2:
        return 360.
    azi = sorted(azi)

    for i in range(1, aziCount):
        gap.append(azi[i] - azi[i-1])
    gap.append(azi[0] - azi[aziCount-1] + 360)
    gap = sorted(gap, reverse=True)
    return sum(gap[0:n])

def azimuths(origin, maxDelta=180, minWeight=0.5):
    """
    Returns a sorted list of azimuths for all arrivals within
    maxDelta and with the specified minimum weight.
    """
    azi = []
    arrivalCount = origin.arrivalCount()
    for i in range(arrivalCount):
        arr = origin.arrival(i)
        try:
            azimuth = arr.azimuth()
            weight  = arr.weight()
            delta   = arr.distance()
        except ValueError:
            continue
        if weight >= minWeight and delta <= maxDelta:
            azimuth = math.fmod(azimuth, 360.)
            if azimuth < 0:
                azimuth += 360.
            if azimuth not in azi:
                azi.append(azimuth)
    return sorted(azi)

def computeAzimuthalGap(origin, maxDelta=180, minWeight=0.5):
    """
    Compute the largest azimuthal gap
    """
    azi = azimuths(origin, maxDelta, minWeight)
    aziCount = len(azi)
    if aziCount < 2:
        return 360.

    azi.append(azi[0] + 360)
    azi = numpy.array(azi)
    gap = azi[1:] - azi[:-1]
    return max(gap)

def computeSecondaryAzimuthalGap(origin, maxDelta=180, minWeight=0.5):
    """
    Compute the secondary azimuthal gap, i.e. the sum of the largest
    two azimuthal gaps separated only by a single station.
    """
    azi = azimuths(origin, maxDelta, minWeight)
    aziCount = len(azi)
    if aziCount < 2:
        return 360.

    azi.append(azi[0] + 360)
    azi = numpy.array(azi)
    gap = azi[1:] - azi[:-1]
    sgap = gap[1:] + gap[:-1]
    return max(sgap)

def computeTGap(origin, maxDelta=180, minWeight=0.5):
    """
    Compute the sum of the largest two gaps. Unlike for the well-known
    secondary azimuthal gap, the TGap does not depend on the number of
    stations that separate two gaps. Also the two largest gaps don't
    have to be adjacent.
    """
    azi = azimuths(origin, maxDelta, minWeight)
    return sumOfLargestGaps(azi, n=2)


def readEventParametersFromXML(xml):
    seiscomp.logging.debug("Reading parametric data from " + xml)

    ar = seiscomp.io.XMLArchive()
    if ar.open(xml) is False:
        raise IOError(xml + ": unable to open")
    obj = ar.readObject()
    if obj is None:
        raise TypeError(xml + ": invalid format")
    ep = seiscomp.datamodel.EventParameters.Cast(obj)
    if ep is None:
        raise TypeError(xml + ": no event parameters found")

    return ep

def readInventoryFromXML(xml):
    seiscomp.logging.debug("Reading inventory from " + xml)

    ar = seiscomp.io.XMLArchive()
    if ar.open(xml) is False:
        raise IOError(xml + ": unable to open")
    obj = ar.readObject()
    if obj is None:
        raise TypeError(xml + ": invalid format")
    inventory = seiscomp.datamodel.Inventory.Cast(obj)
    if inventory is None:
        raise TypeError(xml + ": no inventory found")
    return inventory
