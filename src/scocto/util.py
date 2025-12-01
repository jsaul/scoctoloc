import seiscomp.datamodel
import seiscomp.io
import scstuff.util

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
