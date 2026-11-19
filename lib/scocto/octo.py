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
import scocto.util
import pyproj
import pyocto


origin_count = 0


class PyOctoConfig:
    """
    PyOcto-specific config parameters used by scoctoloc

    We follow the PyOcto parameter naming as closely as possible.
    Not all PyOcto parameters are supported, like the geographic
    projections and related xlim, ylim
    """
    def __init__(self):
        # These are passed to the associator
        self.n_picks = 6
        self.n_p_picks = 4
        self.n_s_picks = 2
        self.n_p_and_s_picks = 2

        self.time_before = 300.

        # VelocityModel0D specification
        self.velocity_model_const = None

        # VelocityModel1D
        self.velocity_model_csv_filename = None

        # Constant velocity
        # used only in PyOcto's VelocityModel0D
        self.p_velocity = 7
        self.s_velocity = 4

        # Surface velocity for elevation correction
        # used only in PyOcto's VelocityModel1D
        self.surface_p_velocity = None
        self.surface_s_velocity = None

        # These are passed to both VelocityModel0D and VelocityModel1D
        self.tolerance = 6.
        self.association_cutoff_distance = 250
        self.location_cutoff_distance = 150
        self.pick_match_tolerance = 6.

        # FIXME: not-yet supported parameters
        self.time_slicing = 1200.
        self.min_node_size = 10.
        self.min_node_size_location = 1.5
        self.min_interevent_time = 3
        self.refinement_iterations = 3
        self.max_pick_overlap = 4
        self.min_pick_fraction = 0.25
        self.exponential_edt = False
        self.edt_pick_std = 1.0
        self.location_split_depth = 6
        self.location_split_return = 4
        self.n_threads = None
        self.node_log_interval = 0
        self.queue_memory_protection_dfs_size = 500000
        self.second_pass_overwrites = None

        # Unsupported/replaced PyOcto parameters:
        # xlim, ylim are replaced by a circular region around (center_lat, center_lon) with max_distance as radius
        # zlim is replaced by min_depth, max_depth


class Config:
    """
    Non-PyOcto-specific config parameters used by scoctoloc
    """
    def __init__(self):
        # These are passed to the associator
        self.pyocto = PyOctoConfig()
        self.center_lat = None
        self.center_lon = None
        self.max_distance = 800

        # Earthquake min, max and default depth
        self.min_depth =   1
        self.max_depth = 100
        self.default_depth = 10

        self.pick_delay = 60.

        self.locator_name = "LOCSAT"

        self.debug_data_dir = None
        # FIXME
        self.debug_data_dir = "debug-dir"


class Associator(pyocto.OctoAssociator):

    def __init__(self, config):
        self.config = config

        # Setup of PyOcto velocity model
        # This velocity model is *only* used in the association.
        if self.config.pyocto.velocity_model_csv_filename:
            if self.config.pyocto.velocity_model_const:
                raise RuntimeError("Cannot use two velocity models at the same time!")
            velocity_model = self.createVelocityModelFromCSV()
        elif self.config.pyocto.velocity_model_const:
            velocity_model = self.createConstantVelocityModel()
        else:
            # Default constant-velocity model
            log.warning("Using default constant-velocity model with vp,vs,rh=7,4,2")
            velocity_model = pyocto.VelocityModel0D(7, 4, 2)

        if self.config.debug_data_dir:
            # If debug_data_dir is specified, debug output is written
            # in a format that can be read directly by PyOcto.
            log.debug("Enabling PyOcto debug output to %s" % str(self.config.debug_data_dir))
            self.debug_data_dir = pathlib.Path(self.config.debug_data_dir)
            self.debug_data_dir.mkdir(mode=0o755, parents=True, exist_ok=True)
        else:
            self.debug_data_dir = None

        crs_local = pyproj.CRS(proj='aeqd', lat_0=self.config.center_lat, lon_0=self.config.center_lon, datum='WGS84', type='crs')

        xlim = (-self.config.max_distance, self.config.max_distance)
        ylim = (-self.config.max_distance, self.config.max_distance)
        zlim = (self.config.min_depth, self.config.max_depth)

        log.debug("  center lat, lon = %.3f, %.3f" % (self.config.center_lat, self.config.center_lon))
        log.debug("  max_distance = %g" % self.config.max_distance)
        log.debug("Initializing pyocto.OctoAssociator")
        log.debug("  xlim = (%f, %f)" % xlim)
        log.debug("  ylim = (%f, %f)" % ylim)
        log.debug("  zlim = (%f, %f)" % zlim)
        log.debug("  time_before = %f" % self.config.pyocto.time_before)
        log.debug("  velocity_model = %s" % str(velocity_model))
        log.debug("  n_picks = %d" % self.config.pyocto.n_picks)
        log.debug("  n_p_picks = %d" % self.config.pyocto.n_p_picks)
        log.debug("  n_s_picks = %d" % self.config.pyocto.n_s_picks)
        log.debug("  n_p_and_s_picks = %d" % self.config.pyocto.n_p_and_s_picks)
        log.debug("  association_cutoff_distance = %g" % self.config.pyocto.association_cutoff_distance)
        log.debug("  location_cutoff_distance = %g" % self.config.pyocto.location_cutoff_distance)
        log.debug("  pick_match_tolerance = %d" % self.config.pyocto.pick_match_tolerance)
        log.debug("  crs = %s" % str(crs_local))

        super().__init__(
            xlim=xlim,
            ylim=ylim,
            zlim=zlim,
            time_before=self.config.pyocto.time_before,
            velocity_model=velocity_model,
            n_picks=self.config.pyocto.n_picks,
            n_p_picks=self.config.pyocto.n_p_picks,
            n_s_picks=self.config.pyocto.n_s_picks,
            n_p_and_s_picks=self.config.pyocto.n_p_and_s_picks,
            pick_match_tolerance=self.config.pyocto.pick_match_tolerance,
            crs=crs_local,)

        # This is a list of only network, station, location codes of the
        # configured sensors, to avoid feeding picks for unconfigured
        # stations.
        self.stream_nsl = []

        # White list of accepted pick authors
        self.accepted_authors = ["scautopick"]

        self.pyocto_picks = None

    def setPickAuthors(self, authors):
        """
        Set whitelist of accepted pick authors
        """
        self.accepted_authors = authors

    def _convertInventoryToPyOcto(self, inventory, whitelist=None):
        """
        Convert a SeisComP inventory to a PyOcto inventory 
        """
        log.debug("Preparing PyOcto inventory")
        stations = pandas.core.frame.DataFrame()

        tmp = dict()
        for item in scocto.util.InventoryIterator(inventory):
            network, station, location = item
            n, s, l = network.code(), station.code(), location.code()
            # Only keep the stations matching the whitelist
            streamid = seiscomp.datamodel.WaveformStreamID(n, s, l, "*", "")
            if whitelist and not whitelist.matches(streamid):
                continue
            delta, az, baz = seiscomp.math.delazi(
                self.config.center_lat, self.config.center_lon,
                location.latitude(), location.longitude())
            if delta*111.195 > self.config.max_distance:
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

    def clear(self):
        """
        Removes acquired picks
        """
        self.pyocto_picks = None

    def feed(self, picks):
        """
        Acquire picks.

        This is incremental and will append picks to existing picks
        if called sequentially.
        """
        filtered_picks = []
        for pick in picks:
            if not self.accepts(pick):
                log.debug("pick " + pick.publicID() + " rejected")
                continue
            filtered_picks.append(pick)

        pyocto_picks = self._preparePicksForPyOcto(filtered_picks)
        if self.pyocto_picks:
            for key in pyocto_picks:
                self.pyocto_picks[key].extend(pyocto_picks[key])
        else:
            self.pyocto_picks = pyocto_picks

    def process(self):
        """
        Process all acquired picks
        """
        # A final check
        pick_count = len(self.pyocto_picks["time"])
        if pick_count < self.config.pyocto.n_p_picks:
            log.debug("Too few picks left %d - stop" % pick_count)
            return []

        log.debug("Processing %d picks" % pick_count)

        # Before processing convert the whole pick dict to a pandas DataFrame
        pyocto_picks = pandas.core.frame.DataFrame()
        pyocto_picks["station"] = self.pyocto_picks["station"]
        pyocto_picks["phase"] = self.pyocto_picks["phase"]
        pyocto_picks["time"] = self.pyocto_picks["time"]
        pyocto_picks["event"] = self.pyocto_picks["event"]
        pyocto_picks["public_id"] = self.pyocto_picks["public_id"]

        pyocto_picks["time"] = pyocto_picks["time"].apply(lambda x: x.timestamp())

        if self.debug_data_dir:
            # This file can be used as input for @yetinam's PyOcto examples,
            # which may be useful for debugging.
            debug_picks_file = self.debug_data_dir / "picks"
            if debug_picks_file.exists():
                for i in range(999):
                    debug_picks_file = self.debug_data_dir / ("picks-%03d" % (i+1))
                    if not debug_picks_file.exists():
                        break
            log.debug("Dumping PyOcto pick data to %s" % (str(debug_picks_file)))
            pyocto_picks.to_parquet(debug_picks_file)

        log.debug("Running associator")
        t0 = time.time()
        pyocto_events, pyocto_assignments = \
            self.associate(pyocto_picks, self.pyocto_stations)
        t1 = time.time()
        log.debug("Association took %.3f seconds" % (t1-t0,))
        if pyocto_events.empty:
            log.debug("No origins")
            return []
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
            origin = self.convertOriginFromPyOcto(ievent, idx, self.pyocto_stations, pyocto_events, pyocto_assignments)
            if origin:
                origins.append(origin)

        return origins

    def setInventory(self, inventory, whitelist=None):
        """
        Set up internal pyocto station list based on the SeisComP inventory
        """
        self.inventory = inventory
        self._convertInventoryToPyOcto(inventory, whitelist)

    def _preparePicksForPyOcto(self, objects):
        log.debug("Preparing pyocto picks")

        allowed_phases = "P", "S"
        default_phase = "P"

        _station  = list()
        _phase = list()
        _time = list()
        _event = list()
        _public_id = list()
        for obj in objects:
            pick = seiscomp.datamodel.Pick.Cast(obj)
            if not pick:
                continue 
            n, s, l, c = scocto.util.nslc(pick)
            _station.append("%s.%s.%s" % (n, s, l))
            try:
                phase = str(pick.phaseHint().code())
            except ValueError:
                phase = default_phase
            if phase not in allowed_phases:
                phase = default_phase
            _phase.append(phase)
            time = scocto.util.time2str(pick.time().value())
            time = pandas.Timestamp(time)
            _time.append(time)
            _event.append(-1)
            _public_id.append(pick.publicID())

        picks = dict()
        picks["station"] = _station
        picks["phase"] = _phase
        picks["time"] = _time
        picks["event"] = _event
        picks["public_id"] = _public_id

        return picks

    def convertOriginFromPyOcto(self, ievent, idx, pyocto_stations, pyocto_events, pyocto_assignments):
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

        n_p_picks = 0
        n_s_picks = 0

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

            # count P and S arrivals
            if phase[0] == "P":
                n_p_picks += 1
            elif phase[0] == "S":
                n_s_picks += 1
            else:
                pass

        n_picks = n_p_picks + n_s_picks

        conf = self.config.pyocto
        if n_p_picks < conf.n_p_picks or \
           n_s_picks < conf.n_s_picks or \
           n_picks   < conf.n_picks:
            log.warning("Origin from PyOcto does not meet minimum-pick count criteria")
            if n_p_picks < conf.n_p_picks:
                log.warning("  n_p_picks: %2d < %2d" % (n_p_picks, conf.n_p_picks))
            if n_s_picks < self.config.pyocto.n_s_picks:
                log.warning("  n_s_picks: %2d < %2d" % (n_s_picks, conf.n_s_picks))
            if n_picks < self.config.pyocto.n_picks:
                log.warning("  n_picks:   %2d < %2d" % (n_picks, conf.n_picks))
            return None

        return origin

    def createConstantVelocityModel(self):
        """
        Create a constant-velocity model from a specification string in the format
        vp, vs, rh
        """
        tolerance = self.config.pyocto.tolerance
        association_cutoff_distance = self.config.pyocto.association_cutoff_distance
        location_cutoff_distance = self.config.pyocto.location_cutoff_distance
        spec_string = self.config.pyocto.velocity_model_const
        values = tuple(map(float, spec_string.split(",")))
        if len(values) == 1:
            vp = values[0]
            vs = vp/(3**0.5)
        elif len(values) == 2:
            vp, vs = values
        else:
            log.error("Failed to parse single layer spec '%s'" % spec_string)
            return False
        # sanity check
        if not 1 < vp < 9 or not 0 < vs < 5.2:
            log.error("Unreasonable velocity values vp, vs = %.3f, %.3f" % (vp, vs))
            return False

        log.debug("Using constant-velocity model with vp, vs = %.3f, %.3f" % (vp, vs))
        return pyocto.VelocityModel0D(
                vp, vs,
                self.config.pyocto.tolerance,
                self.config.pyocto.association_cutoff_distance)
#               self.config.pyocto.location_cutoff_distance)


    def createVelocityModelFromCSV(self):
        csv_filename = self.config.pyocto.velocity_model_csv_filename

        # Grid spacing in kilometer
        delta = 1.

        # Read model from file (requires pyrocko!)

        tmp_path = pathlib.Path("/tmp")
        model_path = tmp_path / "model"
        layers = pandas.read_csv(csv_filename)
        # xdist is the model size. It is twice the max_distance (which is
        # a radius from our network center) and should be larger than the
        # association_cutoff_distance.
        xdist = self.config.max_distance * 2.
        if xdist < self.config.pyocto.association_cutoff_distance:
            raise RuntimeError("Model size %g smaller than association_cutoff_distance %g" % (self.config.max_distance, self.config.pyocto.association_cutoff_distance))

        # zdist ist the model depth. It must be large enough to accommodate
        # the deepest-penetrating rays used for the travel time computation.
        # The max_distance should normally be more than enough.
        zdist = self.config.max_distance
        pyocto.VelocityModel1D.create_model(layers, delta, xdist, zdist, model_path)

        return pyocto.VelocityModel1D(
                model_path,
                self.config.pyocto.tolerance,
                self.config.pyocto.association_cutoff_distance,
# removed       self.config.pyocto.location_cutoff_distance,
                self.config.pyocto.surface_p_velocity,
                self.config.pyocto.surface_s_velocity)
