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

import time
import datetime
import pathlib
import pandas
import seiscomp.logging as log
import seiscomp.datamodel
import scstuff.util
import scstuff.inventory
import pyproj
import pyocto


# Directory where debug data are written to
debug_data_dir = pathlib.Path("debug")


def convertPicksToPyOcto(objects):
    log.debug("Preparing pyocto picks")
    picks = pandas.core.frame.DataFrame()

    _st  = list()
    _ph = list()
    _tm = list()
    _ev = list()
    _id = list()
    for obj in objects:
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if not pick:
            continue 
        n, s, l, c = scstuff.util.nslc(pick)
        _st.append("%s.%s.%s" % (n, s, l))
        try:
            ph = str(pick.phaseHint().code())
        except ValueError:
            ph = "P"
        if not ph:
            ph = "P"
        _ph.append(ph)
        tm = scstuff.util.format_time(pick.time().value())
        tm = pandas.Timestamp(tm)
        _tm.append(tm)
        _ev.append(0)
        _id.append(pick.publicID())
    picks["station"] = _st
    picks["phase"] = _ph
    picks["time"] = _tm
    picks["event"] = [-1]*len(_ev)
    picks["public_id"] = _id

    picks["time"] = picks["time"].apply(lambda x: x.timestamp())

    return picks


origin_count = 0


def convertOriginFromPyocto(ievent, idx, pyocto_stations, pyocto_events, pyocto_assignments):
    global origin_count

    events = pyocto_events
    assign = pyocto_assignments

    origin_count += 1
    public_id = "Origin/PyOcto/%09d" % (origin_count)
    elat = seiscomp.datamodel.RealQuantity( events["latitude"][ievent] )
    elon = seiscomp.datamodel.RealQuantity( events["longitude"][ievent] )
    edep = seiscomp.datamodel.RealQuantity( events["depth"][ievent] )
    etim = events["time"][ievent]
    etim = seiscomp.core.Time() + seiscomp.core.TimeSpan(etim.timestamp())
    etim = seiscomp.datamodel.TimeQuantity(etim)
    origin = seiscomp.datamodel.Origin(public_id)
    origin.setTime(etim)
    origin.setLatitude(elat)
    origin.setLongitude(elon)
    origin.setDepth(edep)
    origin.setMethodID("PyOcto")
    origin.setEvaluationMode(seiscomp.datamodel.AUTOMATIC)
    origin.setEvaluationStatus(seiscomp.datamodel.PRELIMINARY)

    for ipick, event_idx in enumerate(assign["event_idx"]):
        if event_idx != idx:
            continue
        pick_id = assign["public_id"][ipick]
        residual = assign["residual"][ipick]
        phase = assign["phase"][ipick]
        scode = assign["station"][ipick]

        arrival = seiscomp.datamodel.Arrival()
        arrival.setPickID(pick_id)
        arrival.setPhase(seiscomp.datamodel.Phase(phase))
        arrival.setTimeResidual(residual)
        i = pyocto_stations["id"].to_list().index(scode)
        slat = pyocto_stations["latitude"][i]
        slon = pyocto_stations["longitude"][i]
        delta, az, baz = seiscomp.math.delazi(elat.value(), elon.value(), slat, slon)
        arrival.setAzimuth(az)
        arrival.setDistance(delta)

        origin.add(arrival)

    return origin


def createConstantVelocityModel(spec_string):
    """
    Create a constant-velocity model from a specification string in the format
    vp, vs, rh
    """
    values = tuple(map(float, spec_string.split(",")))
    if not 0 < len(values) < 4:
        log.error("failed to parse single layer spec '%s'" % spec_string)
        return False
    vs = rh = None
    if len(values) >= 1:
        vp = values[0]
    if len(values) >= 2:
        vs = values[1]
    if len(values) == 3:
        rh = values[2]
    if not vs:
        vs = vp/(3**0.5)
    if not rh:
        # from Bertheussen, 1977
        rh = 0.77 + 0.32*vp

    log.debug("Using constant-velocity model with vp,vs,rh=%.3f,%.3f,%.3f" % (vp, vs, rh))
    velocity_model = pyocto.VelocityModel0D(vp, vs, rh)
    return velocity_model


def createVelocityModelFromCSV(csv_filename):
    # Read model from file (requires pyrocko!)
    tmp_path = pathlib.Path("/tmp")
    model_path = tmp_path / "model"
    layers = pandas.read_csv(csv_filename)
    pyocto.VelocityModel1D.create_model(layers, 1, self.max_distance, self.max_depth, model_path)
    velocity_model = pyocto.VelocityModel1D(model_path, 2.0)
    return velocity_model


class Associator(pyocto.OctoAssociator):

    def __init__(self,
            center_lat, center_lon, max_dist=200., min_depth=1., max_depth=100.,
            min_num_p_picks=4,
            min_num_s_picks=0,
            min_num_p_and_s_picks=0,
            min_num_p_or_s_picks=4,
            velocity_model=None,
            debug_data_dir=None):
        self.pick_match_tolerance = 6.

        self.center_lat = center_lat
        self.center_lon = center_lon
        self.max_dist  = max_dist
        self.min_depth = min_depth
        self.max_depth = max_depth

        self.min_num_p_picks = min_num_p_picks
        self.min_num_s_picks = min_num_s_picks
        self.min_num_p_and_s_picks = min_num_p_and_s_picks
        self.min_num_p_or_s_picks = min_num_p_or_s_picks

        assert velocity_model is not None

        self.enablePyOctoDebugOutput(debug_data_dir)

        log.debug("Center latitude, longitude = %.3f, %.3f" % (center_lat, center_lon))
        crs_local = pyproj.CRS(proj='aeqd', lat_0=center_lat, lon_0=center_lon, datum='WGS84', type='crs')

        super().__init__(
            xlim=(-self.max_dist, self.max_dist),
            ylim=(-self.max_dist, self.max_dist),
            zlim=(self.min_depth, self.max_depth),
            time_before=300.0,
            velocity_model=velocity_model,
            n_picks=self.min_num_p_or_s_picks,
            n_p_picks=self.min_num_p_picks,
            n_s_picks=self.min_num_s_picks,
            n_p_and_s_picks=self.min_num_p_and_s_picks,
            pick_match_tolerance=self.pick_match_tolerance,
            crs=crs_local,)

        # This is a list of only network, station, location codes of the
        # configured sensors, to avoid feeding picks for unconfigured
        # stations.
        self.stream_nsl = []

        # White list of accepted pick authors
        self.accepted_authors = ["scautopick"]

    def enablePyOctoDebugOutput(self, debug_data_dir):
        self.debug_data_dir = debug_data_dir
        if not self.debug_data_dir:
            return
        self.debug_data_dir = pathlib.Path(debug_data_dir)
        self.debug_data_dir.mkdir(mode=0o755, parents=True, exist_ok=True)

    def setPickAuthors(self, authors):
        """
        Set whitelist of accepted pick authors
        """
        self.accepted_authors = authors

    def convertInventoryToPyOcto(self, inventory, whitelist=None):
        log.debug("Preparing pyocto inventory")
        stations = pandas.core.frame.DataFrame()

        tmp = dict()
        for item in scstuff.inventory.InventoryIterator(inventory):
            network, station, location, stream = item
            n, s, l = network.code(), station.code(), location.code()
            # Only keep the stations matching the whitelist
            streamid = seiscomp.datamodel.WaveformStreamID(n, s, l, "*", "")
            if whitelist and not whitelist.matches(streamid):
                continue
            delta, az, baz = seiscomp.math.delazi(
                self.center_lat, self.center_lon,
                location.latitude(), location.longitude())
            if delta*111.195 > self.max_dist:
                continue

            tmp[n, s, l] = (location.latitude(), location.longitude(), location.elevation())

            # Keep track of the sensors that passed the whitelist
            # and/or geographical selection criteria. Picks from all
            # other sensors will be rejected.
            nsl = (n, s, l)
            if nsl not in self.stream_nsl:
                self.stream_nsl.append(nsl)

        _st  = list()
        _lat = list()
        _lon = list()
        _ele = list()
        for n, s, l in tmp:
            latitude, longitude, elevation = tmp[n, s, l]
            _id = "%s.%s.%s" % (n, s, l)
            log.debug(_id)
            _st.append(_id)
            _lat.append(latitude)
            _lon.append(longitude)
            _ele.append(elevation)
        stations["id"] = _st
        stations["latitude"] = _lat
        stations["longitude"] = _lon
        stations["elevation"] = _ele

        self.pyocto_stations = stations
        if self.debug_data_dir:
            self.pyocto_stations.to_parquet(self.debug_data_dir / "stations")
        self.transform_stations(self.pyocto_stations)

    def setupConstantVelocityModel(self, vp, vs, rh):
        velocity_model = pyocto.VelocityModel0D(vp, vs, rh)
        return velocity_model

    def accepts(self, pick):
        w = pick.waveformID()
        nsl = (w.networkCode(), w.stationCode(), w.locationCode())
        if nsl not in self.stream_nsl:
            return False

        author = pick.creationInfo().author()
        if self.accepted_authors and author not in self.accepted_authors:
            return False

        return True

    def process(self, picks):
        filtered_picks = []
        for pick in picks:
            if not self.accepts(pick):
                log.debug("pick " + pick.publicID() + " rejected")
                continue
            filtered_picks.append(pick)
        if len(filtered_picks) < self.min_num_p_picks:
            log.debug("Too few picks left %d - stop" % (len(filtered_picks)))
            return []

        pyocto_picks = convertPicksToPyOcto(filtered_picks)
        if self.debug_data_dir:
            # This file can be used as input for @yetinam's PyOcto examples,
            # which may be useful for debugging.
            pyocto_picks.to_parquet(self.debug_data_dir / "picks")

        log.debug("Running associator")
        t0 = time.time()
        pyocto_events, pyocto_assignments = \
            self.associate(pyocto_picks, self.pyocto_stations)
        if pyocto_events.empty:
            log.debug("No origins")
            return []
        t1 = time.time()
        log.debug("Association took %.3f seconds" % (t1-t0,))
        log.debug("Generated %d origins using %d picks" % (
            len(pyocto_events), len(pyocto_assignments)))

        self.transform_events(pyocto_events)
        pyocto_events["time"] = pyocto_events["time"].apply(
            datetime.datetime.fromtimestamp, tz=datetime.timezone.utc)
        pyocto_assignments["time"] = pyocto_assignments["time"].apply(
            datetime.datetime.fromtimestamp, tz=datetime.timezone.utc)

        del pyocto_events["x"]
        del pyocto_events["y"]
        del pyocto_events["z"]
        del pyocto_assignments["event"]
        del pyocto_assignments["pick_idx"]
        del pyocto_assignments["time"]

        log.debug("#### origins\n" + str(pyocto_events))

        origins = list()
        for ievent, idx in enumerate(pyocto_events["idx"]):
            origin = convertOriginFromPyocto(ievent, idx, self.pyocto_stations, pyocto_events, pyocto_assignments)
            origins.append(origin)

        return origins

    def setInventory(self, inventory, whitelist=None):
        """
        Set up internal pyocto station list based on the SeisComP inventory
        """
        self.inventory = inventory
        self.convertInventoryToPyOcto(inventory, whitelist)
