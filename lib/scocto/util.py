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
import math
import numpy


def time2str(time, digits=3):
    """
    Convert a seiscomp.core.Time to a string
    """
    return time.toString("%Y-%m-%d %H:%M:%S.%f000000")[:20+digits].strip(".")


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


def parseTime(s):
    for fmtstr in "%FT%TZ", "%FT%T.%fZ", "%F %T", "%F %T.%f":
        t = seiscomp.core.Time.GMT()
        if t.fromString(s, fmtstr):
            return t
    raise ValueError("could not parse time string '%s'" %s)


def creationTime(obj):
    """ For readability """
    return obj.creationInfo().creationTime()


def pickTime(pick):
    """ For readability """
    return pick.time().value()


def nslc(obj):
    """
    Convenience function to retrieve network, station, location and
    channel codes from a waveformID object and return them as tuple
    """
    if isinstance(obj, seiscomp.datamodel.WaveformStreamID):
        n = obj.networkCode()
        s = obj.stationCode()
        l = obj.locationCode()
        c = obj.channelCode()
    else:
        return nslc(obj.waveformID())
    return n, s, l, c


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
        n, s, l, c = nslc(pick.waveformID())
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


def originDistanceKm(origin1, origin2):
    lat1 = origin1.latitude().value()
    lat2 = origin2.latitude().value()
    lon1 = origin1.longitude().value()
    lon2 = origin2.longitude().value()
    dep1 = origin1.depth().value()
    dep2 = origin2.depth().value()
    delta, az, baz = seiscomp.math.delazi_wgs84(lat1, lon1, lat2, lon2)
    delta_km = delta*111.195
    dist_km = (delta_km**2 + (dep2-dep1)**2)**0.5
    return dist_km


def originTimeSeparation(origin1, origin2):
    dt = float(origin2.time().value() - origin1.time().value())
    return abs(dt)


def compareOrigins(a, b):
    """
    Compare origin a and b in terms of referenced picks.

    Returns either 0 (no improvement) or 1 (improvement)
    """
    pick_ids_a = list()
    pick_ids_b = list()

    for i in range(a.arrivalCount()):
        arr = a.arrival(i)
        pick_ids_a.append(arr.pickID())
    for i in range(b.arrivalCount()):
        arr = b.arrival(i)
        pick_ids_b.append(arr.pickID())

    pick_ids_a.sort()
    pick_ids_b.sort()
    if pick_ids_a == pick_ids_b:
        return 0

    common_pick_count = 0
    for pick_id in pick_ids_a:
        if pick_id in pick_ids_b:
            common_pick_count += 1
    if not common_pick_count:
        return -1

    if common_pick_count == len(pick_ids_a):
        if len(pick_ids_b) > len(pick_ids_a):
            return 1
        else:
            return 0

    if len(pick_ids_b) > len(pick_ids_a):
        return 1
    if len(pick_ids_b) < len(pick_ids_a):
        return -1

    seiscomp.logging.warning("Same number of picks but not same picks")
    for pick_id in pick_ids_a:
        if pick_id not in pick_ids_b:
            seiscomp.logging.warning("Pick in A not B: " + pick_id)
    for pick_id in pick_ids_b:
        if pick_id not in pick_ids_a:
            seiscomp.logging.warning("Pick in B not A: " + pick_id)
    return -1


def readEventParametersFromXML(xml):
    """
    Read the entire content of the specified XML file into one
    EventParameters instance.
    """
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
    """
    Read the entire content of the specified XML file into one
    Inventory instance.
    """
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


def filterObjects(objects, authorWhitelist=None, agencyWhitelist=None):

    def inrange(obj):
        try:
            c = obj.creationInfo()
        except ValueError:
            return False
    
        if authorWhitelist is not None and c.author() not in authorWhitelist:
            return False

        if agencyWhitelist is not None and c.agencyID() not in agencyWhitelist:
            return False

        return True

    def inrange_dict(item):
        key, obj = item
        return inrange(obj)

    def inrange_list(item):
        return inrange(item)

    if isinstance(objects, dict):
        filtered = filter(inrange_dict, objects.items())
    else:
        filtered = filter(inrange_list, objects)

    return type(objects)(filtered)


def loadPicksForTimespan(
    query, startTime, endTime, authors=None):
    """
    Load from the database all picks within the given time span. If specified,
    also all amplitudes that reference any of these picks may be returned.
    """

    seiscomp.logging.debug("loading picks for %s ... %s" % (
        time2str(startTime), time2str(endTime)))

    if authors:
        seiscomp.logging.debug("using author whitelist: " + str(", ".join(authors)))

    objects = dict()

    for obj in query.getPicks(startTime, endTime):
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if pick:
            objects[pick.publicID()] = pick

    seiscomp.logging.debug("Loaded %d picks from database" % len(objects))
    objects = filterObjects(objects, authorWhitelist=authors)

    return objects.values()


def EventParametersPicks(ep):
    """
    Iterate over the picks in the EventParameters instance ep
    """
    for i in range(ep.pickCount()):
        obj = seiscomp.datamodel.Pick.Cast(ep.pick(i))
        if obj:
            yield obj


def InventoryIterator(inventory, time=None):
    """
    inventory is a SeisComP inventory instance. Note that this needs
    to be an inventory incl. the streams. Otherwise this iterator
    makes no sense.
    """

    for inet in range(inventory.networkCount()):
        network = inventory.network(inet)
        if time is not None and not operational(network, time):
            continue

        for ista in range(network.stationCount()):
            station = network.station(ista)

            if time is not None and not operational(station, time):
                continue

            for iloc in range(station.sensorLocationCount()):
                location = station.sensorLocation(iloc)

                if time is not None and not operational(location, time):
                    continue

                for istr in range(location.streamCount()):
                    stream = location.stream(istr)

                    if time is not None and not operational(stream, time):
                        continue

                    yield network, station, location, stream


def PublicObjectCast(obj):
    for tp in [
        seiscomp.datamodel.Amplitude,
        seiscomp.datamodel.Pick,
        seiscomp.datamodel.Magnitude,
        seiscomp.datamodel.Origin,
        seiscomp.datamodel.FocalMechanism,
        seiscomp.datamodel.Event
        ]:
        typedObject = tp.Cast(obj)
        if typedObject:
            return typedObject
    return obj
