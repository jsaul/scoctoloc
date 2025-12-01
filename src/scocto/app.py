import sys

import pyrocko.modelling

import seiscomp.client
import seiscomp.core
import seiscomp.datamodel
import seiscomp.io
import seiscomp.logging
import seiscomp.math
import scstuff.util
import scstuff.dbutil

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
        self.origins = list()

        # Dict of all picks referenced by this event
        self.picks = dict()

        # The currently preferred origin; usually self.origins[-1]
        self.preferredOrigin = None

        self.last_published = False

    def set_origin(self, origin, picks):
        self.origins.append(origin)
        for i in range(origin.arrivalCount()):
            arr = origin.arrival(i)
            self.picks = picks[arr.pickID()]


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
    dt = origin2.time().value() - origin1.time().value()
    return abs(dt)


class MyEventList(list):

    def find_matching_event(self, origin):

        pick_ids = list()
        for i in range(origin.arrivalCount()):
            arr = origin.arrival(i)
            pick_ids.append(arr.pickID())

        matching_events = list()
        for event in self:
            if not event.origins:
                continue

            last = event.origins[-1]
            if origin_time_separation(last, origin) < 30 and \
               origin_distance_km(last, origin) < 100:

                common_pick_count = 0

                for pick_id in pick_ids:
                    if pick_id in event.picks:
                        common_pick_count += 1

                if common_picks:
                    matching_events.append( (common_pick_count, event) )

        if matching_events:
            common_pick_count, matching_event = sorted(matching_events)[-1]
            return matching_event


class App(seiscomp.client.Application):

    def __init__(self, argc, argv):
        super().__init__(argc, argv)
        self.setRecordStreamEnabled(False)
        self.setLoadInventoryEnabled(True)

        self.objects = list()
        self.picks = dict()

        self.inventory_xml = None
        self.model_csv = None
        self.debug = False
        self.test = False
        self.origin_count = 0
        self.center_latlon = None
        self.max_distance = 500.
        self.max_depth = 100.

        self.min_num_p_picks = 4
        self.min_num_s_picks = 0
        self.min_num_p_and_s_picks = 0
        self.min_num_p_or_s_picks = 4

        self.use_pick_time = False
        self.target_messaging_group = "LOCATION"
        self.pick_authors = ["scautopick*"]

        self.event_list = MyEventList()

    def createCommandLineDescription(self):
        self.commandline().addGroup("Input")
        self.commandline().addStringOption("Input", "input-xml", "specify input xml file")
        self.commandline().addStringOption("Input", "inventory-xml", "specify inventory xml file")
        self.commandline().addStringOption("Input", "model-csv", "specify velocity model csv file")
        self.commandline().addStringOption("Input", "start-time", "specify start time")
        self.commandline().addStringOption("Input", "end-time", "specify end time")
        self.commandline().addGroup("Config")
        self.commandline().addStringOption("Config", "pick-authors", "specify list of allowed pick authors")
        self.commandline().addStringOption("Config", "whitelist", "specify stream whitelist")
        self.commandline().addStringOption("Config", "center-latlon", "specify network center lat,lon")
        self.commandline().addStringOption("Config", "max-distance", "specify network radius from center lat,lon")
        self.commandline().addStringOption("Config", "max-depth", "specify max. hypocenter depth in km")
        self.commandline().addOption("Config", "test", "test mode - no results are sent to messaging")

        self.commandline().addGroup("Playback")
        self.commandline().addOption("Playback", "playback", "run in playback mode")
        self.commandline().addOption("Playback", "use-pick-time", "use pick time as playback time")
        self.commandline().addGroup("Output")
        self.commandline().addStringOption("Output", "output-xml", "specify output xml file")
        self.commandline().addStringOption("Output", "output-schedule", "specify output schedule in seconds after origin time as comma separated values")
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
            center = self.configGetStrings("scoctoloc.center")
            assert len(center) == 2
            self.center_latlon = tuple(map(float, center))
        except RuntimeError:
            pass

        try:
            self.max_distance = self.configGetString("scoctoloc.maxDistance")
        except RuntimeError:
            pass

        try:
            self.max_depth = self.configGetString("scoctoloc.maxDepth")
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
            self.playback_mode = True
        else:
            self.playback_mode = False

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
            self.model_csv = self.commandline().optionString("model-csv")
        except RuntimeError:
            self.model_csv = None

        try:
            tmp = self.commandline().optionString("center-latlon")
            self.center_latlon = tuple(map(float, tmp.split(",")))
        except RuntimeError:
            self.center_latlon = None

        try:
            tmp = self.commandline().optionString("max-distance")
            self.max_distance = float(tmp)
        except RuntimeError:
            self.max_distance = 500.

        try:
            tmp = self.commandline().optionString("max-depth")
            self.max_depth = float(tmp)
        except RuntimeError:
            self.max_depth = 100.

        if self.commandline().hasOption("use-pick-time"):
            self.use_pick_time = True

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

        if self.commandline().hasOption("messaging-group"):
            self.target_messaging_group = self.commandline().optionString("messaging-group")

        if self.commandline().hasOption("debug"):
            self.debug = True

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

    def loadInputData(self):
        """
        Load input data from database or XML file and return a list of objects.

        This is for offline processing or playback. In online processing mode new
        objects are received from the messaging via addObject().
        """
        if self.input_xml:
            # offline mode
            self.ep = scocto.util.readEventParametersFromXML(self.input_xml)
            objects = dict()
            for obj in scstuff.util.EventParametersPicks(self.ep):
                objects[obj.publicID()] = seiscomp.datamodel.Pick.Cast(obj)
        else:
            # database query in online mode, no EventParameters to read from/write to
            self.ep = None
            objects = scstuff.dbutil.loadPicksForTimespan(self.query(), self.start_time, self.end_time)

        if self.whitelist:
            objects = [
                obj for obj in objects.values()
                if self.whitelist.matches(obj.waveformID()) ]
        else:
            objects = [obj for obj in objects.values()]

        return objects

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
                max_dist=self.max_distance,
                max_depth=self.max_depth,
                min_num_p_picks=self.min_num_p_picks,
                min_num_s_picks=self.min_num_s_picks,
                min_num_p_and_s_picks=self.min_num_p_and_s_picks,
                min_num_p_or_s_picks=self.min_num_p_or_s_picks,
                velocity_model_csv=self.model_csv,
                debug=self.debug)
        associator.setInventory(self.inventory)
        associator.setPickAuthors(self.pick_authors)
        self.associator = associator

        return True

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
        seiscomp.logging.debug("Running in offline mode")

        origins = self.associator.process(self.objects)

        if self.debug:
            for origin in origins:
                s = scocto.util.printOrigin(origin, self.objects)
                seiscomp.logging.info(s)

        ep = self.ep
        for origin in origins:
            ep.add(origin)

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
        self.objects.sort(key=lambda x: objectTime(x))

        for obj in self.objects:
            self.addObject("", obj)

        return True

    def init(self):

        if not super().init():
            return False

        if self.center_latlon is None:
            raise RuntimeError("Must specify center-latlon")

        self.setupStreamWhitelist()
        self.setupInventory()
        self.setupAssociators()


        return True

    def run(self):
        """
        This is the main routine.

        We either
        - run this once and return (offline mode) or
        - hand over to Application.run() and collect new objects via addObject()
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
            self.ep = None
            objects = scstuff.dbutil.loadPicksForTimespan(self.query(), self.start_time, self.end_time)
        else:
            objects = None

        if objects:
            # This is for offline processing or playback.
            if self.whitelist:
                objects = [
                    obj for obj in objects.values()
                    if self.whitelist.matches(obj.waveformID()) ]
            else:
                objects = [obj for obj in objects.values()]

            self.objects = objects

            if self.playback_mode:
                seiscomp.logging.debug("Running in playback mode")
                return self.runPlayback()
            else:
                seiscomp.logging.debug("Running in offline mode")
                return self.runOffline()

        # Run in online processing mode.
        # New objects will be received from the messaging via addObject().
        seiscomp.logging.debug("Running in online mode")
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
            # seiscomp.logging.debug(msg)
        else:
            msg = msg + "no match with stream whitelist -> stop"
            # seiscomp.logging.debug(msg)
        return matches

    def checkPick(self, new_pick):
        if not self.checkPickAuthor(new_pick):
            return False

        if not self.checkStation(new_pick):
            return False

        if not self.associator.accepts(new_pick):
            # seiscomp.logging.debug("pick " + new_pick.publicID() + " rejected by associator")
            return False

    def storePick(self, new_pick):
        self.picks[new_pick.publicID()] = new_pick
        return True

    def processPick(self, new_pick):
        seiscomp.logging.info("Processing pick " + new_pick.publicID())

        # Process pick in the context of other picks within a small time window
        dt = seiscomp.core.TimeSpan(60)
        tmin = new_pick.time().value() - dt
        tmax = new_pick.time().value() + dt

        # Is the pick *already* in our objects buffer?
        # If not, we are in messaging mode; otherwise playback mode.
        if new_pick in self.objects:
            playback = True
        else:
            playback = False

        if playback:
            # offline/playback mode
            assert self.playback_mode
            tstr = scocto.util.time2str(scocto.util.creationTime(new_pick))
            seiscomp.logging.debug("Playback time is " + tstr)
            picks = [p for p in self.objects if tmin < p.time().value() < tmax ]
            picks = [p for p in picks if scocto.util.creationTime(p) <= scocto.util.creationTime(new_pick)]
        else:
            # online mode
            assert not self.playback_mode
            self.objects.append(new_pick)
            picks = [p for p in self.objects if tmin < p.time().value() < tmax]

        if self.debug and len(picks) > 1:
            seiscomp.logging.debug("Number of picks in vicinity: %d" % (len(picks)))
            for pick in sorted(picks, key=lambda p: p.time().value()):
                dt = pick.time().value() - new_pick.time().value()
                try:
                    ph = str(pick.phaseHint().code())
                except ValueError:
                    ph = "?"

                seiscomp.logging.debug("%+7.3f %s %s" % (dt, ph, pick.publicID()))

        if len(picks) < self.min_num_p_picks:
            return

        origins = self.associator.process(picks)
        if not origins:
            return

        filtered_origins = []

        for origin in origins:
            if not scocto.util.originReferencesPick(origin, new_pick):
                msg = "new origin " + origin.publicID() + \
                    " doesn't reference new pick"
                seiscomp.logging.debug(msg)
                seiscomp.logging.debug("Dismissing this origin")
                continue

            filtered_origins.append(origin)

        origins = filtered_origins

        for origin in origins:
            if self.debug:
                seiscomp.logging.debug("new origin " + origin.publicID())
                s = scocto.util.printOrigin(origin, self.objects)
                seiscomp.logging.info(s)

            matching_event = self.event_list.find_matching_event(origin)

            if matching_event:
                seiscomp.logging.debug("matching event found")
            else:
                seiscomp.logging.debug("new event")
                matching_event = MyEvent()

            matching_event.set_origin(origin, self.picks)

            if matching_event.last_published:
                pass

        if not playback:
            ep = seiscomp.datamodel.EventParameters()
            seiscomp.datamodel.Notifier.Enable()
            for origin in origins:
                newPublicID = seiscomp.datamodel.Origin.Create().publicID().replace("/","/PyOcto/")
                origin.setPublicID(newPublicID)
                now = seiscomp.core.Time.UTC()
                ci = seiscomp.datamodel.CreationInfo()
                ci.setAgencyID("GFZ")
                ci.setAuthor("scoctoloc")
                ci.setCreationTime(now)
                origin.setCreationInfo(ci)
                ep.add(origin)
            msg = seiscomp.datamodel.Notifier.GetMessage()
            seiscomp.datamodel.Notifier.Disable()

        if playback or self.commandline().hasOption("test"):
            for origin in origins:
                seiscomp.logging.info("test mode - not sending " + origin.publicID())
        else:
            if self.connection().send(msg):
                for origin in origins:
                    seiscomp.logging.info("sent " + origin.publicID())
            else:
                for origin in origins:
                    seiscomp.logging.info("failed to send " + origin.publicID())

        return True

    def addPick(self, new_pick):
        """
        Feed a new pick to the processing
        """
        if not self.checkPick(new_pick):
            return

        if not self.storePick(new_pick):
            return

        if not self.processPick(new_pick):
            return

    def addObject(self, parentID, obj):
        """
        Add a new object just received from the messaging
        """
        pick = seiscomp.datamodel.Pick.Cast(obj)
        if pick:
            self.addPick(pick)


def main():
    app = App(len(sys.argv), sys.argv)
    return app()


if __name__ == "__main__":
    main()
