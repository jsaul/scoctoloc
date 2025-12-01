import time
import datetime
import pathlib
import pandas
import seiscomp.logging
import seiscomp.datamodel
import seiscomp.seismology
import scstuff.util
import scstuff.inventory
import pyproj
import pyocto


# Directory where debug data are written to
debug_dir = pathlib.Path("debug")


def convertPicksToPyOcto(objects):
    seiscomp.logging.debug("Preparing pyocto picks")
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
        seiscomp.logging.debug(pick.publicID())
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


class Associator(pyocto.OctoAssociator):

    def __init__(self,
            center_lat, center_lon, max_dist=1000., max_depth=250.,
            min_num_p_picks=4,
            min_num_s_picks=0,
            min_num_p_and_s_picks=0,
            min_num_p_or_s_picks=4,
            velocity_model_csv=None,
            debug=False):

        self.center_lat = center_lat
        self.center_lon = center_lon
        self.max_dist  = max_dist
        self.max_depth = max_depth

        self.min_num_p_picks = min_num_p_picks
        self.min_num_s_picks = min_num_s_picks
        self.min_num_p_and_s_picks = min_num_p_and_s_picks
        self.min_num_p_or_s_picks = min_num_p_or_s_picks

        velocity_model = self.setupVelocityModel(velocity_model_csv)

        self.debug = debug

        seiscomp.logging.debug("Center latitude, longitude = %.3f, %.3f" % (center_lat, center_lon))
        crs_local = pyproj.CRS(proj='aeqd', lat_0=center_lat, lon_0=center_lon, datum='WGS84', type='crs')

        super().__init__(
            xlim=(-self.max_dist, self.max_dist),
            ylim=(-self.max_dist, self.max_dist),
            zlim=(0.0, self.max_depth),
            time_before=300.0,
            velocity_model=velocity_model,
            n_picks=self.min_num_p_or_s_picks,
            n_p_picks=self.min_num_p_picks,
            n_s_picks=self.min_num_s_picks,
            n_p_and_s_picks=self.min_num_p_and_s_picks,
            pick_match_tolerance=3.0,
            crs=crs_local,)

        if self.debug:
            debug_dir.mkdir(mode=0o755, parents=False, exist_ok=True)

        # This is a list of only network, station, location codes of the
        # configured sensors, to avoid feeding picks for unconfigured
        # stations.
        self.stream_nsl = []

        # White list of accepted pick authors
        self.accepted_authors = ["scautopick"]

        self._locatorInterface = seiscomp.seismology.LocatorInterface.Create("LOCSAT")

    def setPickAuthors(self, authors):
        """
        Set whitelist of accepted pick authors
        """
        self.accepted_authors = authors

    def convertInventoryToPyOcto(self, inventory, whitelist=None):
        seiscomp.logging.debug("Preparing pyocto inventory")
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
            seiscomp.logging.debug(_id)
            _st.append(_id)
            _lat.append(latitude)
            _lon.append(longitude)
            _ele.append(elevation)
        stations["id"] = _st
        stations["latitude"] = _lat
        stations["longitude"] = _lon
        stations["elevation"] = _ele

        self.pyocto_stations = stations
        if self.debug:
            self.pyocto_stations.to_parquet(debug_dir / "stations")
        self.transform_stations(self.pyocto_stations)

    def setupVelocityModel(self, model_csv=None):
        if model_csv:
            # Read model from file (requires pyrocko!)
            tmp_path = pathlib.Path("/tmp")
            model_path = tmp_path / "model"
            layers = pandas.read_csv(model_csv)
            pyocto.VelocityModel1D.create_model(layers, 1, self.max_dist, self.max_depth, model_path)
            velocity_model = pyocto.VelocityModel1D(model_path, 2.0)
        else:
            velocity_model = pyocto.VelocityModel0D(7.0, 4.0, 2.0)

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

    def relocate(self, origin):
        relocated = None
        fixedDepth = None
        minDepth = 1.

        loc = self._locatorInterface

        def deepCloneOrigin(origin):
            cloned = seiscomp.datamodel.Origin.Cast(origin.clone())
            for iarr in range(origin.arrivalCount()):
                arr = seiscomp.datamodel.Arrival.Cast(origin.arrival(iarr).clone())
                cloned.add(arr)
            return cloned

        origin = deepCloneOrigin(origin)

        seiscomp.logging.debug("Before arrival loop")
        for iarr in range(origin.arrivalCount()):
            arr = origin.arrival(iarr)
            arr.setWeight(1)
            arr.setTimeUsed(True)
        seiscomp.logging.debug("After  arrival loop")

        while True:
            if fixedDepth is None:
                loc.useFixedDepth(False)
                seiscomp.logging.info("Using free depth")
            else:
                loc.useFixedDepth(True)
                loc.setFixedDepth(fixedDepth)
                seiscomp.logging.info("Using fixed depth of %g km" % fixedDepth)

            now = seiscomp.core.Time.GMT()

            try:
                relocated = loc.relocate(origin)
                relocated = seiscomp.datamodel.Origin.Cast(relocated)
                seiscomp.logging.debug("Relocation succeeded")
            except RuntimeError:
                relocated = None
                seiscomp.logging.debug("Relocation failed")
                
            if relocated:
                if fixedDepth is None:
                    relocated.setDepthType(seiscomp.datamodel.FROM_LOCATION)
                else:
                    relocated.setDepthType(seiscomp.datamodel.OPERATOR_ASSIGNED)

                if relocated.depth().value() < minDepth and fixedDepth is None:
                    # Fix depth to minimum depth and relocate again
                    fixedDepth = minDepth
                    continue

            break

        if relocated:
            try:
                quality = relocated.originQuality()
            except:
                quality = seiscomp.datamodel.OriginQuality()
            quality.setAssociatedPhaseCount(relocated.arrivalCount())
            quality.setUsedPhaseCount(relocated.arrivalCount())
            relocated.setQuality(quality)
            return relocated

    def process(self, picks):
        filtered_picks = []
        for pick in picks:
            if not self.accepts(pick):
                seiscomp.logging.debug("pick " + pick.publicID() + " rejected")
                continue
            filtered_picks.append(pick)
        if len(filtered_picks) < self.min_num_p_picks:
            seiscomp.logging.debug("Too few picks left %d - stop" % (len(filtered_picks)))
            return []

        pyocto_picks = convertPicksToPyOcto(filtered_picks)
        if self.debug:
            # This file can be used as input for @yetinam's PyOcto examples,
            # which may be useful for debugging.
            pyocto_picks.to_parquet(debug_dir / "picks")

        seiscomp.logging.debug("Running associator")
        t0 = time.time()
        pyocto_events, pyocto_assignments = \
            self.associate(pyocto_picks, self.pyocto_stations)
        if pyocto_events.empty:
            seiscomp.logging.debug("No origins")
            return []
        t1 = time.time()
        seiscomp.logging.debug("Association took %.3f seconds" % (t1-t0,))
        seiscomp.logging.debug("Generated %d origins using %d picks" % (
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

        seiscomp.logging.debug("#### origins\n" + str(pyocto_events))
        seiscomp.logging.debug("#### assignments\n" + str(pyocto_assignments))

        origins = list()
        for ievent, idx in enumerate(pyocto_events["idx"]):
            origin = convertOriginFromPyocto(ievent, idx, self.pyocto_stations, pyocto_events, pyocto_assignments)
            origins.append(origin)
            relocated = self.relocate(origin)
            if relocated:
                origins.append(relocated)

        return origins

    def setInventory(self, inventory, whitelist=None):
        """
        Set up internal pyocto station list based on the SeisComP inventory
        """
        self.inventory = inventory
        self.convertInventoryToPyOcto(inventory, whitelist)
