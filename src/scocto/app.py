import sys

import pyrocko.modelling

import seiscomp.client
import seiscomp.core
import seiscomp.datamodel
import seiscomp.seismology
import seiscomp.io
import seiscomp.logging
import seiscomp.math
import scstuff.util
import scstuff.dbutil

import numpy
import pyocto
import scocto.octo
import scocto.util
import scocto.whitelist


def creationTime(obj):
    """ For readability """
    return obj.creationInfo().creationTime()


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


def origin_distance_km(origin1, origin2):
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


def origin_time_separation(origin1, origin2):
    dt = float(origin2.time().value() - origin1.time().value())
    return abs(dt)


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
            if origin_time_separation(last, origin) < 30 and \
               origin_distance_km(last, origin) < 100:

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


class App(seiscomp.client.Application):

    def __init__(self, argc, argv):
        super().__init__(argc, argv)
        self.setRecordStreamEnabled(False)
        self.setLoadInventoryEnabled(True)

        self.picks = dict()
        self.sortedPicks = list()

        self.offlineBuffer = list()

        self.inventory_xml = None
        self.modelCSV = None
        self.modelConst = None
        self.test = False
        self.center_latlon = None

        self.maxDistance = 300.
        self.maxDepth = 100.

        self.processingMode = "online"

        self._locator_name = "LOCSAT"

        self.min_num_p_picks = 4
        self.min_num_s_picks = 0
        self.min_num_p_and_s_picks = 0
        self.min_num_p_or_s_picks = 4

        self.want_raw_pyocto_locations = False

        self.use_pick_time = False
        self.target_messaging_group = "LOCATION"
        self.pick_authors = ["scautopick*"]

        self.pick_queue = list()
        self.pick_delay = 0
        self.pick_delay = 180

        self.debug_data_dir = None

        self.event_list = MyEventList()

        self.playbackTime = None

    def createCommandLineDescription(self):
        self.commandline().addGroup("Input")
        self.commandline().addStringOption("Input", "input-xml", "specify input xml file")
        self.commandline().addStringOption("Input", "inventory-xml", "specify inventory xml file")
        self.commandline().addStringOption("Input", "model-csv", "specify velocity model csv file")
        self.commandline().addStringOption("Input", "model-const", "specify P velocity[, S velocity[, density]]")
        self.commandline().addStringOption("Input", "start-time", "specify start time")
        self.commandline().addStringOption("Input", "end-time", "specify end time")
        self.commandline().addGroup("Config")
        self.commandline().addStringOption("Config", "pick-authors", "specify list of allowed pick authors")
        self.commandline().addStringOption("Config", "whitelist", "specify stream whitelist")
        self.commandline().addStringOption("Config", "center-latlon", "specify network center lat,lon")
        self.commandline().addDoubleOption("Config", "max-distance", "specify network radius from center lat,lon")
        self.commandline().addDoubleOption("Config", "max-depth", "specify max. hypocenter depth in km")
        self.commandline().addDoubleOption("Config", "pick-delay", "specify pick processing delay in seconds (experimental)")
        self.commandline().addStringOption("Config", "locator", "specify locator (default is LOCSAT)")
        self.commandline().addOption("Config", "test", "test mode - no results are sent to messaging")
        self.commandline().addOption("Config", "debug-data-dir", "specify folder to dump input for debugging in PyOcto (off by default)")

        self.commandline().addGroup("Playback")
        self.commandline().addOption("Playback", "playback", "run in playback mode")
        self.commandline().addOption("Playback", "use-pick-time", "use pick time as playback time")
        self.commandline().addGroup("Output")
        self.commandline().addStringOption("Output", "output-xml", "specify output xml file")
        self.commandline().addStringOption("Output", "output-schedule", "specify output schedule in seconds after origin time as comma separated values")
        self.commandline().addOption("Output", "pyocto-locations", "produce raw PyOcto locations (before relocation)")
        return True

    def initConfiguration(self):
        if not super().initConfiguration():
            return False

        # Input config

        try:
            self.target_messaging_group = self.configGetString("scoctoloc.messagingGroup")
        except RuntimeError:
            pass

        try:
            self.pick_authors = self.configGetStrings("scoctoloc.pickAuthors")
        except RuntimeError:
            pass

        # Associator config

        try:
            center = self.configGetStrings("scoctoloc.network.center")
            assert len(center) == 2
            self.center_latlon = tuple(map(float, center))
        except RuntimeError:
            pass

        try:
            self.maxDistance = self.configGetDouble("scoctoloc.network.radius")
        except RuntimeError:
            pass

        try:
            self.modelCSV = self.configGetString("scoctoloc.octo.model.csv")
        except RuntimeError:
            self.modelCSV = None

        try:
            # Constant-velocity, single layer
            self.modelConst = self.configGetString("scoctoloc.octo.model.const")
        except RuntimeError:
            self.modelConst = None

        try:
            self.maxDepth = self.configGetString("scoctoloc.maxDepth")
        except RuntimeError:
            pass

        try:
            self.min_num_p_picks = self.configGetInt("scoctoloc.minPickCountP")
        except RuntimeError:
            pass

        try:
            self.min_num_s_picks = self.configGetInt("scoctoloc.minPickCountS")
        except RuntimeError:
            pass

        try:
            self.min_num_p_and_s_picks = self.configGetInt("scoctoloc.minPickCountPAndS")
        except RuntimeError:
            pass

        try:
            self.min_num_p_or_s_picks = self.configGetInt("scoctoloc.minPickCountPOrS")
        except RuntimeError:
            pass

        try:
            self.pick_delay = self.configGetDouble("scoctoloc.pickDelay")
        except RuntimeError:
            pass

        # Locator config

        try:
            self._locator_name = self.configGetString("scoctoloc.locator")
        except RuntimeError:
            pass

        # Output config

        try:
            self.output_schedule = self.configGetStrings("scoctoloc.outputSchedule")
        except RuntimeError:
            pass

        return True

    def validateParameters(self):
        if super().validateParameters() is False:
            return False

        if self.commandline().hasOption("input"):
            self.setDatabaseEnabled(False, False)
            self.setMessagingEnabled(False)
        else:
            self.setDatabaseEnabled(True, True)
            if self.commandline().hasOption("database"):
                self.setMessagingEnabled(False)
            else:
                self.setMessagingEnabled(True)

        if self.commandline().hasOption("playback"):
            self.processingMode = "playback"
        else:
            pass

        try:
            self._locator_name = self.commandline().optionString("locator")
        except RuntimeError:
            pass

        try:
            self.input_xml = self.commandline().optionString("input-xml")
        except RuntimeError:
            self.input_xml = None

        try:
            self.inventory_xml = self.commandline().optionString("inventory-xml")
        except RuntimeError:
            self.inventory_xml = None

        try:
            self.output_xml = self.commandline().optionString("output-xml")
        except RuntimeError:
            self.output_xml = "-"

        try:
            self.modelCSV = self.commandline().optionString("model-csv")
        except RuntimeError:
            pass

        try:
            # Constant-velocity, single layer
            self.modelConst = self.commandline().optionString("model-const")
        except RuntimeError:
            pass

        try:
            tmp = self.commandline().optionString("center-latlon")
            self.center_latlon = tuple(map(float, tmp.split(",")))
        except RuntimeError:
            self.center_latlon = None

        try:
            self.maxDistance = self.commandline().optionDouble("max-distance")
        except RuntimeError:
            pass

        try:
            self.maxDepth = self.commandline().optionDouble("max-depth")
        except RuntimeError:
            pass

        if self.commandline().hasOption("use-pick-time"):
            self.use_pick_time = True

        try:
            tmp = self.commandline().optionString("pick-delay")
            self.pick_delay = float(tmp)
        except RuntimeError:
            pass

        try:
            self.pick_authors = self.commandline().optionString("pick-authors").replace(",", " ").split()
        except RuntimeError:
            pass

        try:
            self.output_schedule = self.commandline().optionString("output-schedule").replace(",", " ").split()
        except RuntimeError:
            self.output_schedule = []

        try:
            start_time = self.commandline().optionString("start-time")
            end_time = self.commandline().optionString("end-time")

            self.start_time = scstuff.util.parseTime(start_time)
            self.end_time = scstuff.util.parseTime(end_time)
        except RuntimeError:
            self.start_time = self.end_time = None

        if self.commandline().hasOption("pyocto-locations"):
            self.want_raw_pyocto_locations = True

        if self.commandline().hasOption("messaging-group"):
            self.target_messaging_group = self.commandline().optionString("messaging-group")

        try:
            self.debug_data_dir = self.commandline().optionString("debug-data-dir")
        except RuntimeError:
            pass

        self.output_schedule = [float(t) for t in self.output_schedule]

        self.setupMessagingAndDatabase()

        return True

    def setupInventory(self):
        """
        Load SeisComP station inventory from database or XML file and
        keep it as self.inventory.

        Returns True if successful, False otherwise.
        """
        if self.inventory_xml:
            self.inventory = scocto.util.readInventoryFromXML(self.inventory_xml)
        else:
            self.inventory = seiscomp.client.Inventory.Instance().inventory()

        return True

    def setupMessagingAndDatabase(self):
        if self.input_xml:
            self.setDatabaseEnabled(False, False)
            self.setMessagingEnabled(False)
        else:
            self.setDatabaseEnabled(True, True)
            self.setMessagingEnabled(True)
            self.addMessagingSubscription("PICK")
            self.addMessagingSubscription("MLTEST")
            self.addMessagingSubscription(self.target_messaging_group)
            self.setPrimaryMessagingGroup(self.target_messaging_group)

    def setupStreamWhitelist(self):
        if self.commandline().hasOption("whitelist"):
            filename = self.commandline().optionString("whitelist")
            seiscomp.logging.debug("Reading stream whitelist from " + filename)
            self.whitelist = scocto.whitelist.StreamWhitelist(filename)
        else:
            self.whitelist = None

    def setupAssociators(self):

        if self.center_latlon is not None:
            center_lat, center_lon = self.center_latlon
        else:
            pass

        seiscomp.logging.debug("Setting up associator")
        associator = scocto.octo.Associator(
                center_lat, center_lon,
                max_dist=self.maxDistance,
                max_depth=self.maxDepth,
                min_num_p_picks=self.min_num_p_picks,
                min_num_s_picks=self.min_num_s_picks,
                min_num_p_and_s_picks=self.min_num_p_and_s_picks,
                min_num_p_or_s_picks=self.min_num_p_or_s_picks,
                velocity_model=self.velocityModel,
                debug_data_dir=self.debug_data_dir)
        associator.setInventory(self.inventory)
        associator.setPickAuthors(self.pick_authors)
        self.associator = associator

        return True

    def setupLocator(self, name):
        seiscomp.logging.debug("Setting up locator " + name)
        self._locatorInterface = seiscomp.seismology.LocatorInterface.Create(name)
        seiscomp.logging.debug("Finished locator setup")

    def relocate(self, origin):

        relocated = None
        fixedDepth = None
        minDepth = 1.

        assert self._locatorInterface is not None
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
                try:
                    quality = relocated.originQuality()
                except:
                    quality = seiscomp.datamodel.OriginQuality()

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

            residuals = []
            distances = []
            associatedPhaseCount = 0
            usedPhaseCount = 0
            for i in range(relocated.arrivalCount()):
                arrival = relocated.arrival(i)
                residual = arrival.timeResidual()
                residuals.append(residual)
                distance = arrival.distance()
                distances.append(distance)
                if arrival.timeUsed():
                    usedPhaseCount += 1
                associatedPhaseCount += 1
            rms = numpy.sqrt(sum(numpy.array(residuals) ** 2)/len(residuals))
            quality.setStandardError(rms)
            quality.setAssociatedPhaseCount(associatedPhaseCount)
            quality.setUsedPhaseCount(usedPhaseCount)
            distances.sort()
            quality.setMinimumDistance(distances[0])
            quality.setMaximumDistance(distances[-1])
            quality.setMedianDistance(numpy.median(distances))
            relocated.setQuality(quality)
            return relocated

    def relocateOrigins(self, origins):
        relocated_origins = list()
        for origin in origins:
            relocated = self.relocate(origin)
            if relocated:
                relocated_origins.append(relocated)
        origins.extend(relocated_origins)

        if not self.want_raw_pyocto_locations:
            discarded_origins = [origin for origin in origins if origin.methodID() == "PyOcto"]
            for origin in discarded_origins:
                origins.remove(origin)

    def process(self, objects):
        origins = self.associator.process(objects)

        return origins

    def runOffline(self):
        """
        This is the offline processing (not playback) workflow

        - Load Inventory from file
        - Load EventParameters from file
        - Convert everything internally to PyOcto objects
        - Run the PyOcto associator
        - From the newly nucleated origins and the associations
          update the EventParameters in place
        - Write back EventParameters to file
        - done
        """
        origins = self.process(self.offlineBuffer)

        self.relocateOrigins(origins)

        ep = self.ep
        for origin in origins:
            ep.add(origin)

        seiscomp.logging.debug("Writing output to %s" % self.output_xml)
        ar = seiscomp.io.XMLArchive()
        ar.setFormattedOutput(True)
        ar.create(self.output_xml)
        ar.writeObject(ep)
        ar.close()

        return True

    def runPlayback(self):
        """
        Another offline processing workflow, but here the picks
        are fed into the processing sequentially, one by one, in
        the order of either their creation time (which is the
        default) or optionally their pick time.

        This allows simulation of the real-time behaviour of the
        associator/locator. If playback order is determined by
        pick time (--use-pick-time), then also picks created much
        later (e.g. due to data acquisition latency or post
        processing) can be played back like in real time.

        - Load EventParameters from file
        - Sort objects by creation time (or optionally pick time)
        - Feed the objects to the processing via addObject
        - For each fed pick run the PyOcto associator
        - If an origin could be generated, it is saved to produce
          a history of the origins as function of time. The origin
          creation time will be the creation time of the current pick
          plus epsilon.
        - Finally write back EventParameters to file
        """
        objectTime = scocto.util.pickTime if self.use_pick_time else scocto.util.creationTime

        # Sort objects by creation time
        self.offlineBuffer.sort(key=lambda x: objectTime(x))

        for pick in self.offlineBuffer:
            seiscomp.logging.debug(pick.publicID())

        for obj in self.offlineBuffer:
            self.addObject("", obj)

        # Process any remaining picks
        self.processPickQueue()

        seiscomp.logging.debug("Writing output to %s" % self.output_xml)
        ar = seiscomp.io.XMLArchive()
        ar.setFormattedOutput(True)
        ar.create(self.output_xml)
        ar.writeObject(self.ep)
        ar.close()

        return True

    def init(self):

        if not super().init():
            return False

        if self.center_latlon is None:
            raise RuntimeError("Must specify center-latlon")

        if self.modelCSV:
            if self.modelConst:
                raise RuntimeError("Cannot use two velocity models at the same time!")
            self.velocityModel = scocto.octo.createVelocityModelFromCSV(self.modelCSV)
        elif self.modelConst:
            self.velocityModel = scocto.octo.createConstantVelocityModel(self.modelConst)
        else:
            seiscomp.logging.warning("Using default constant-velocity model with vp,vs,rh=7,4,2")
            self.velocityModel = pyocto.VelocityModel0D(7, 4, 2)

        self.setupStreamWhitelist()
        self.setupInventory()
        self.setupAssociators()

        self.setupLocator(self._locator_name)

        return True

    def loadInputData(self):
        """
        Load input data from database or XML file and return a list of objects.

        This is for offline processing or playback. In online processing mode new
        objects are received from the messaging via addObject().
        """

        if self.input_xml and self.inventory_xml:
            self.ep = scocto.util.readEventParametersFromXML(self.input_xml)
            objects = dict()
            for obj in scstuff.util.EventParametersPicks(self.ep):
                pick = seiscomp.datamodel.Pick.Cast(obj)
                if self.start_time is not None and self.end_time is not None:
                    if not self.start_time <= scocto.util.pickTime(pick) <= self.end_time:
                        continue
                objects[obj.publicID()] = pick
        elif self.start_time is not None and self.end_time is not None:
            # database query in online mode, no EventParameters to read from/write to
            self.ep = seiscomp.datamodel.EventParameters()
            objects = scstuff.dbutil.loadPicksForTimespan(self.query(), self.start_time, self.end_time)
            for key in objects:
                obj = objects[key]
                if obj:
                    self.ep.add(obj)
        else:
            objects = dict()

        if objects:
            objects = [ obj for obj in objects.values() if self.checkPick(obj) ]

        return objects

    def prepareOfflineRun(self):
        """
        Prepare offline processing or playback.
        """
        objects = self.loadInputData()

        self.offlineBuffer = objects

    def run(self):
        """
        This is the main routine.

        We either
        - run this once and return (offline mode) or
        - hand over to Application.run() and collect new objects via addObject()
        """

        seiscomp.logging.debug("Running in " + self.processingMode + " mode")

        if self.processingMode != "online":
            self.prepareOfflineRun()

            if not self.offlineBuffer:
                seiscomp.logging.error("No objects read!")
                return False

            if self.processingMode == "playback":
                return self.runPlayback()
            elif self.processingMode == "offline":
                return self.runOffline()
            else:
                seiscomp.logging.error("Wrong processing mode " + self.processingMode)
                return False


        timeout_interval = 1
        self.enableTimer(timeout_interval)

        return super().run()

    def checkPickAuthor(self, pick):
        # Author check
        matches = False
        pick_author = pick.creationInfo().author()
        for allowed_pick_author in self.pick_authors:
            if pick_author == allowed_pick_author:
                matches = True
                break
        if matches:
            return True
        msg = "pick %s %s -> stop" % (
            pick.publicID(),
            "author '%s' not in %s" % (pick_author, str(self.pick_authors))
            )
        # seiscomp.logging.debug(msg)
        return False

    def checkStation(self, pick):
        # Station whitelist match
        matches = self.whitelist.matches(pick.waveformID())
        msg = "pick " + pick.publicID()
        if matches:
            msg = msg + " matches stream whitelist"
        else:
            msg = msg + " no match with stream whitelist -> stop"
        # seiscomp.logging.debug(msg)
        return matches

    def checkPick(self, new_pick):
        if not self.checkStation(new_pick):
            return False

        if not self.checkPickAuthor(new_pick):
            return False

        if not self.associator.accepts(new_pick):
            # seiscomp.logging.debug("pick " + new_pick.publicID() + " rejected by associator")
            return False

        return True

    def storePick(self, pick):
        self.picks[pick.publicID()] = pick
        self.sortedPicks.append(pick)
        objectTime = scocto.util.pickTime
        self.sortedPicks.sort(key=lambda x: objectTime(x))
        self.pick_queue.append(pick)

        if self.processingMode == "playback":
            pickCreationTime = scocto.util.creationTime(pick)
            if self.playbackTime is None:
                self.playbackTime = pickCreationTime
            else:
                self.playbackTime = max(self.playbackTime, pickCreationTime)
            seiscomp.logging.debug("Playback time is now %s" % (scocto.util.time2str(self.playbackTime)))
        return True

    def now(self):
        if self.processingMode == "online":
            return seiscomp.core.Time.UTC()
        else:
            return self.playbackTime

    def processPickQueue(self):
        now = self.now()
        processed_picks = list()
        for pick in self.pick_queue:
            if float(now - scocto.util.pickTime(pick)) < self.pick_delay:
                # pick not yet due
                continue
            self.processPick(pick)
            processed_picks.append(pick)
        for pick in processed_picks:
            self.pick_queue.remove(pick)

        return True

    def processPick(self, new_pick):
        seiscomp.logging.info("Processing pick " + new_pick.publicID())
        if self.processingMode == "playback":
            tstr = scocto.util.time2str(scocto.util.creationTime(new_pick))
            seiscomp.logging.debug("Playback time is " + tstr)

        # Process pick in the context of other picks within a small time window
        dt = seiscomp.core.TimeSpan(120 + self.pick_delay)
        tmin = new_pick.time().value() - dt
        tmax = new_pick.time().value() + dt
        time = scocto.util.pickTime
        picks = [p for p in self.sortedPicks if tmin < time(p) < tmax]

        # debugging only
        if len(picks) > 1:
            seiscomp.logging.debug("Number of picks in vicinity: %d" % (len(picks)))
            for pick in picks:
                dt = time(pick) - time(new_pick)
                try:
                    ph = str(pick.phaseHint().code())
                except ValueError:
                    ph = "?"

                seiscomp.logging.debug("%+7.3f %s %s" % (dt, ph, pick.publicID()))

        if len(picks) < self.min_num_p_picks:
            return

        origins = self.process(picks)
        if not origins:
            return

        filtered_origins = []

        for origin in origins:
            if not scocto.util.originReferencesPick(origin, new_pick):
                msg = "new origin " + origin.publicID() + \
                    " doesn't reference new pick " + new_pick.publicID()
                seiscomp.logging.debug(msg)
                seiscomp.logging.debug("Dismissing this origin")
                continue

            filtered_origins.append(origin)

        origins = filtered_origins

        self.relocateOrigins(origins)

        discarded_origins = list()

        for origin in origins:
            seiscomp.logging.debug("new origin " + origin.publicID())
            s = scocto.util.printOrigin(origin, self.sortedPicks)
            seiscomp.logging.info(s)

            matching_event = self.event_list.find_matching_event(origin)

            method = origin.methodID()

            if matching_event:
                seiscomp.logging.debug("matching event found")
                last = matching_event.origins[method][-1]
                if compareOrigins(last, origin) > 0:
                    seiscomp.logging.debug("improvement: %d -> %d" % (last.arrivalCount(), origin.arrivalCount()))
                else:
                    seiscomp.logging.debug("no improvement - skipping origin")
                    discarded_origins.append(origin)
                    continue
            else:
                seiscomp.logging.debug("new event")
                seiscomp.logging.debug("improvement: %d -> %d" % (0, origin.arrivalCount()))
                matching_event = MyEvent()
                self.event_list.append(matching_event)

            matching_event.set_origin(origin, self.picks)

            if matching_event.lastPublished:
                pass

        for discarded_origin in discarded_origins:
            origins.remove(discarded_origin)

        for origin in origins:
            if origin.methodID() == "PyOcto":
                newPublicID = seiscomp.datamodel.Origin.Create().publicID().replace("/", "/PyOcto/")
                origin.setPublicID(newPublicID)
            now = self.now()
            ci = seiscomp.datamodel.CreationInfo()
            ci.setAgencyID(self.agencyID())
            ci.setAuthor("scoctoloc")
            ci.setCreationTime(now)
            origin.setCreationInfo(ci)
            origin.setEvaluationMode(seiscomp.datamodel.AUTOMATIC)

        if self.processingMode == "playback":
            for origin in origins:
                self.ep.add(origin)
        else:
            ep = seiscomp.datamodel.EventParameters()
            seiscomp.datamodel.Notifier.Enable()
            for origin in origins:
                ep.add(origin)
            msg = seiscomp.datamodel.Notifier.GetMessage()
            seiscomp.datamodel.Notifier.Disable()

        if self.processingMode != "online" or self.commandline().hasOption("test"):
            for origin in origins:
                seiscomp.logging.info("test/offline/playback mode - not sending " + origin.publicID())
        else:
            if self.connection().send(msg):
                for origin in origins:
                    seiscomp.logging.info("sent " + origin.publicID())
            else:
                for origin in origins:
                    seiscomp.logging.info("failed to send " + origin.publicID())

        return True

    def addPick(self, pick):
        """
        Feed a new pick to the processing
        """
        if not self.checkPick(pick):
            return

        if not self.storePick(pick):
            return

        if not self.processPickQueue():
            return

    def addObject(self, parentID, obj):
        """
        Add a new object just received from the messaging
        """
        if self.isExitRequested():
            return
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if pick:
            self.addPick(pick)

    def cleanup(self):
        pass

    def handleTimeout(self):
        self.processPickQueue()
        self.cleanup()

def main():
    app = App(len(sys.argv), sys.argv)
    return app()


if __name__ == "__main__":
    main()
