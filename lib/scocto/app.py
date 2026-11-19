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

import sys
import pathlib
import numpy

import pyrocko.modelling

import seiscomp.client
import seiscomp.core
import seiscomp.datamodel
import seiscomp.seismology
import seiscomp.io
import seiscomp.logging
import seiscomp.math

import scocto.octo
import scocto.util
import scocto.event
import scocto.whitelist


def anycast(obj, types=[]):
    """
    Cast a SeisComP Object to any Object subclass specified in types,
    which is a list of types like Pick, Amplitide etc.

    This may also serve as a type filter as only the types specified will
    be considered.

    The types are tried in the order specified, so if you expect many
    Pick objects and want to process them, specify them early in the list.
    """
    for t in types:
        if t.Cast(obj):
            return t.Cast(obj)


class App(seiscomp.client.Application):

    def __init__(self, argc, argv):
        super().__init__(argc, argv)
        self.setRecordStreamEnabled(False)
        self.setLoadInventoryEnabled(True)
        self.setAutoApplyNotifierEnabled(True)  # enabled by default

        # Main Associator config incl. PyOcto config
        self.config = scocto.octo.Config()
        self.startTime = self.endTime = None
        self.want_raw_pyocto_locations = False
        self.targetMessagingGroup = "LOCATION"
        self.messagingGroups = [ "PICK" ]

        self.picks = dict()
        self.sortedPicks = list()
        self.pickQueue = list()
        self.usePickTime = False
        self.pickAuthors = None
        # Set empty phase hint to "P"
        self.defaultPhaseHint = seiscomp.datamodel.Phase()
        self.defaultPhaseHint.setCode("P")

        self.inventoryXML = None

        self.processingMode = "online"
        self.test = False
        self.batchLength = None

        self.playbackTime = None
        # This dict holds all picks that got associated to
        # an origin during a playback. This way we make sure
        # that the picks are present in the same XML file as
        # the origins.
        self.playbackAssociatedPicks = dict()

        self.eventList = scocto.event.MyEventList()

    def createCommandLineDescription(self):
        self.commandline().addGroup("Input")
        self.commandline().addStringOption("Input", "input-xml", "specify input xml file")
        self.commandline().addStringOption("Input", "inventory-xml", "specify inventory xml file")
        self.commandline().addStringOption("Input", "model-csv", "specify velocity model csv file")
        self.commandline().addStringOption("Input", "model-const", "specify P velocity[, S velocity[, density]]")
        self.commandline().addStringOption("Input", "start-time", "specify start time")
        self.commandline().addStringOption("Input", "end-time", "specify end time")
        self.commandline().addDoubleOption("Input", "batch-length", "specify batch time window length in seconds")
        self.commandline().addGroup("Config")
        self.commandline().addStringOption("Config", "pick-authors", "specify list of allowed pick authors")
        self.commandline().addStringOption("Config", "whitelist", "specify stream whitelist")
        self.commandline().addStringOption("Config", "center-latlon", "specify network center lat,lon")
        self.commandline().addDoubleOption("Config", "max-distance", "specify network radius from center lat,lon")
        self.commandline().addDoubleOption("Config", "min-depth", "specify min. hypocenter depth in km")
        self.commandline().addDoubleOption("Config", "max-depth", "specify max. hypocenter depth in km")
        self.commandline().addDoubleOption("Config", "association-cutoff-distance", "specify max. association distance in km")
        self.commandline().addDoubleOption("Config", "location-cutoff-distance", "specify max. location distance in km")
        self.commandline().addDoubleOption("Config", "pick-match-tolerance", "specify max. pick-match tolerance")
        self.commandline().addDoubleOption("Config", "pick-delay", "specify pick processing delay in seconds (experimental)")
        self.commandline().addStringOption("Config", "locator", "specify locator (default is LOCSAT)")
        self.commandline().addOption("Config", "test", "test mode - no results are sent to messaging")

        self.commandline().addGroup("Mode")
        self.commandline().addOption("Mode", "playback", "run in playback mode")
        self.commandline().addOption("Mode", "dump-picks", "dump picks to xml and exit")
        self.commandline().addOption("Mode", "use-pick-time", "when running in playback mode, use pick time as playback time")
        self.commandline().addGroup("Output")
        self.commandline().addStringOption("Output", "output-xml", "specify output xml file")
        self.commandline().addOption("Output", "pyocto-locations", "produce raw PyOcto locations (before relocation)")
        self.commandline().addStringOption("Output", "debug-data-dir", "produce PyOcto debug data")
        return True

    def initConfiguration(self):
        if not super().initConfiguration():
            return False

        # Input config

        try:
            self.targetMessagingGroup = self.configGetString("scoctoloc.targetMessagingGroup")
        except RuntimeError:
            pass

        try:
            self.messagingGroups = self.configGetStrings("scoctoloc.messagingGroups")
        except RuntimeError:
            pass

        try:
            self.pickAuthors = self.configGetStrings("scoctoloc.pickAuthors")
        except RuntimeError:
            pass

        # Associator config

        try:
            latlon = self.configGetStrings("scoctoloc.network.center")
            assert len(latlon) == 2
            self.config.center_lat, self.config.center_lon = \
                tuple(map(float, latlon))
        except RuntimeError:
            pass

        try:
            self.config.min_depth = self.configGetDouble("scoctoloc.minDepth")
        except RuntimeError:
            pass

        try:
            self.config.max_depth = self.configGetDouble("scoctoloc.maxDepth")
        except RuntimeError:
            pass

        try:
            self.config.max_distance = self.configGetDouble("scoctoloc.network.radius")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.velocity_model_csv_filename = self.configGetString("scoctoloc.octo.model.csv")
        except RuntimeError:
            pass

        try:
            # Constant-velocity, single layer
            self.config.pyocto.velocity_model_const = self.configGetString("scoctoloc.octo.model.const")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.tolerance = \
                self.configGetDouble("scoctoloc.pyocto.tolerance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.association_cutoff_distance = \
                self.configGetDouble("scoctoloc.pyocto.association_cutoff_distance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.location_cutoff_distance = \
                self.configGetDouble("scoctoloc.pyocto.location_cutoff_distance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.pick_match_tolerance = \
                self.configGetDouble("scoctoloc.pyocto.pick_match_tolerance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.n_picks = \
                self.configGetInt("scoctoloc.pyocto.n_picks")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.n_p_picks = \
                self.configGetInt("scoctoloc.pyocto.n_p_picks")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.n_s_picks = \
                self.configGetInt("scoctoloc.pyocto.n_s_picks")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.n_p_and_s_picks = \
                self.configGetInt("scoctoloc.pyocto.n_p_and_s_picks")
        except RuntimeError:
            pass

        try:
            self.config.pick_delay = \
                self.configGetDouble("scoctoloc.pickDelay")
        except RuntimeError:
            pass

        # Locator config

        try:
            self.config.locator_name = self.configGetString("scoctoloc.locator")
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

        if self.commandline().hasOption("dump-picks"):
            self.processingMode = "dump picks"
        else:
            pass

        try:
            self.config.locator_name = self.commandline().optionString("locator")
        except RuntimeError:
            pass

        try:
            self.inputXML = self.commandline().optionString("input-xml")
        except RuntimeError:
            self.inputXML = None

        try:
            self.inventoryXML = self.commandline().optionString("inventory-xml")
        except RuntimeError:
            self.inventoryXML = None

        try:
            self.outputXML = self.commandline().optionString("output-xml")
        except RuntimeError:
            self.outputXML = "-"

        try:
            self.config.pyocto.velocity_model_csv_filename = self.commandline().optionString("model-csv")
        except RuntimeError:
            pass

        try:
            # Constant-velocity, single layer
            self.config.pyocto.velocity_model_const = self.commandline().optionString("model-const")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.association_cutoff_distance = self.commandline().optionDouble("association-cutoff-distance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.location_cutoff_distance = self.commandline().optionDouble("location-cutoff-distance")
        except RuntimeError:
            pass

        try:
            self.config.pyocto.pick_match_tolerance = self.commandline().optionDouble("pick-match-tolerance")
        except RuntimeError:
            pass

        try:
            latlon = self.commandline().optionString("center-latlon")
            self.config.center_lat, self.config.center_lon = \
                tuple(map(float, latlon.split(",")))
        except RuntimeError:
            pass

        try:
            self.config.min_depth = self.commandline().optionDouble("min-depth")
        except RuntimeError:
            pass

        try:
            self.config.max_depth = self.commandline().optionDouble("max-depth")
        except RuntimeError:
            pass

        try:
            self.config.max_distance = self.commandline().optionDouble("max-distance")
        except RuntimeError:
            pass

        if self.commandline().hasOption("use-pick-time"):
            self.usePickPime = True

        try:
            self.config.pick_delay = self.commandline().optionDouble("pick-delay")
        except RuntimeError:
            pass

        try:
            self.pickAuthors = self.commandline().optionString("pick-authors").replace(",", " ").split()
        except RuntimeError:
            pass

        try:
            startTime = self.commandline().optionString("start-time")
            endTime = self.commandline().optionString("end-time")

            self.startTime = scocto.util.parseTime(startTime)
            self.endTime   = scocto.util.parseTime(endTime)
        except RuntimeError:
            pass

        try:
            self.batchLength = self.commandline().optionDouble("batch-length")
        except RuntimeError:
            pass

        if self.commandline().hasOption("pyocto-locations"):
            self.want_raw_pyocto_locations = True

        if self.commandline().hasOption("messaging-groups"):
            self.messagingGroups = self.commandline().optionString("messaging-groups").replace(",", " ").split()

        if self.commandline().hasOption("target-messaging-group"):
            self.targetMessagingGroup = self.commandline().optionString("target-messaging-group")

        try:
            self.config.debug_data_dir = self.commandline().optionString("debug-data-dir")
        except RuntimeError:
            pass

        self.setupMessagingAndDatabase()

        return True

    def setupInventory(self):
        """
        Load SeisComP station inventory from database or XML file and
        keep it as self.inventory.

        Returns True if successful, False otherwise.
        """
        if self.inventoryXML:
            self.inventory = scocto.util.readInventoryFromXML(self.inventoryXML)
        else:
            self.inventory = seiscomp.client.Inventory.Instance().inventory()

        return True

    def setupMessagingAndDatabase(self):

        if self.inputXML:
            self.setDatabaseEnabled(False, False)
            self.setMessagingEnabled(False)
        else:
            self.setDatabaseEnabled(True, True)
            self.setMessagingEnabled(True)
            for messagingGroup in self.messagingGroups:
                self.addMessagingSubscription(messagingGroup)
            for group in self.messagingGroups:
                self.addMessagingSubscription(group)
            self.addMessagingSubscription(self.targetMessagingGroup)
            self.setPrimaryMessagingGroup(self.targetMessagingGroup)

    def _deriveCenterLatLonFromInventory(self):
        """
        Compute the approximate center of the network
        """
        lats = []
        lons = []
        if self.inventory is None:
            return None

        for inet in range(self.inventory.networkCount()):
            net = self.inventory.network(inet)
            for ista in range(net.stationCount()):
                sta = net.station(ista)
                try:
                    lats.append(sta.latitude())
                    lons.append(sta.longitude())
                except ValueError:
                    continue

        if not lats:
            return None

        return sum(lats) / len(lats), sum(lons) / len(lons)

    def setupStreamWhitelist(self):
        if self.commandline().hasOption("whitelist"):
            filename = self.commandline().optionString("whitelist")
            seiscomp.logging.debug("Reading stream whitelist from " + filename)
            self.whitelist = scocto.whitelist.StreamWhitelist.FromFile(filename)
        else:
            self.whitelist = None

    def setupAssociators(self):

        seiscomp.logging.debug("Setting up associator")
        associator = scocto.octo.Associator(self.config)
        associator.setInventory(self.inventory)
        associator.setPickAuthors(self.pickAuthors)
        self.associator = associator

        return True

    def setupLocator(self):
        seiscomp.logging.debug("Setting up locator " + self.config.locator_name)
        self.locatorInterface = seiscomp.seismology.LocatorInterface.Create(self.config.locator_name)
        assert self.locatorInterface is not None
        seiscomp.logging.debug("Finished locator setup")

    def populateMetaData(self, origin):
        if origin.methodID() == "PyOcto":
             newPublicID = seiscomp.datamodel.Origin.Create().publicID().replace("/", "/PyOcto/")
             origin.setPublicID(newPublicID)
        ci = seiscomp.datamodel.CreationInfo()
        ci.setCreationTime(self.now())
        ci.setAgencyID(self.agencyID())
        ci.setAuthor(self.author())
        origin.setCreationInfo(ci)
        origin.setEvaluationMode(seiscomp.datamodel.AUTOMATIC)
        seiscomp.logging.debug("populateMetaData origin " + origin.publicID())

    def populateOriginQuality(self, origin):
        try:
            quality = origin.originQuality()
        except:
            quality = seiscomp.datamodel.OriginQuality()

        residuals = []
        distances = []
        associatedPhaseCount = 0
        usedPhaseCount = 0
        for i in range(origin.arrivalCount()):
            arrival = origin.arrival(i)
            distance = arrival.distance()
            distances.append(distance)
            if arrival.timeUsed():
                usedPhaseCount += 1
                residual = arrival.timeResidual()
                residuals.append(residual)
            associatedPhaseCount += 1
        rms = numpy.sqrt(sum(numpy.array(residuals) ** 2)/len(residuals))
        quality.setStandardError(rms)
        quality.setAssociatedPhaseCount(associatedPhaseCount)
        quality.setUsedPhaseCount(usedPhaseCount)
        distances.sort()
        quality.setMinimumDistance(distances[0])
        quality.setMaximumDistance(distances[-1])
        quality.setMedianDistance(numpy.median(distances))
        agap = scocto.util.computeAzimuthalGap(origin)
        sgap = scocto.util.computeSecondaryAzimuthalGap(origin)
        quality.setAzimuthalGap(agap)
        quality.setSecondaryAzimuthalGap(sgap)
        origin.setQuality(quality)

    def relocate(self, origin):
        relocated = None
        fixedDepth = None

        assert self.locatorInterface is not None
        loc = self.locatorInterface

        origin = scocto.util.deepCloneOrigin(origin)

        # If timeUsed hasn't been set, set it to True and a weight of 1.
        for iarr in range(origin.arrivalCount()):
            arr = origin.arrival(iarr)
            try:
                arr.timeUsed()
            except ValueError:
                arr.setTimeUsed(True)
                arr.setWeight(1)

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

                if relocated.depth().value() < self.config.min_depth and fixedDepth is None:
                    # Fix depth to minimum depth and relocate again
                    fixedDepth = self.config.min_depth
                    continue

                if relocated.depth().value() > self.config.max_depth and fixedDepth is None:
                    # Fix depth to maximum depth and relocate again
                    fixedDepth = self.config.max_depth
                    continue

            break

        if relocated:
            self.populateOriginQuality(relocated)
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

        for origin in origins:
            self.populateMetaData(origin)



    def init(self):

        if not super().init():
            return False

        if self.processingMode not in ["playback", "dump picks"]:
            if self.startTime is not None:
                self.processingMode = "offline"

        if self.config.center_lat is None or self.config.center_lon is None:
            latlon = self._deriveCenterLatLonFromInventory()
            if latlon:
                self.config.center_lat, self.config.center_lon = latlon
            else:
                raise RuntimeError("Must specify center-latlon or provide station coordinates in inventory")

        self.setupStreamWhitelist()
        self.setupInventory()
        self.setupAssociators()

        if self.processingMode == "dump picks":
            return True

        self.setupLocator()

        return True

    def _loadInputObjects(self, startTime, endTime):
        """
        Load input data from database or XML file and return a list of (typed) objects.

        This is for offline processing or playback. In online processing mode new
        objects are received from the messaging via addObject().
        """

        if self.inputXML and self.inventoryXML:
            ep = scocto.util.readEventParametersFromXML(self.inputXML)
            objects = list()
            objectTime = scocto.util.pickTime
            for pick in scocto.util.EventParametersPicks(ep):
                if self.startTime is not None and self.endTime is not None:
                    if not startTime <= objectTime(pick) <= endTime:
                        continue
                objects.append(pick)
        elif self.startTime is not None and self.endTime is not None:
            objects = scocto.util.loadPicksForTimespan(self.query(), startTime, endTime)
        else:
            objects = list()

        for obj in objects:
            if scocto.util.isPick(obj):
                phaseHint = scocto.util.phaseHint(obj)
                if phaseHint not in ["P", "S"]:
                    obj.setPhaseHint(self.defaultPhaseHint)

        objects = [obj for obj in objects if self._checkPick(obj)]
        seiscomp.logging.debug("Keeping %d picks" % len(objects))

        return objects

    def run(self):
        """
        This is the main routine.

        We either
        - run this once and return (offline mode) or
        - hand over to Application.run() and collect new objects via addObject()
        """

        seiscomp.logging.debug("Running in " + self.processingMode + " mode")
        seiscomp.logging.debug("pickDelay = %g" % self.config.pick_delay)

        if self.processingMode == "online":

            timeout_interval = 1
            self.enableTimer(timeout_interval)

            return super().run()

        # Treatment of all non-online modes

        self.ep = seiscomp.datamodel.EventParameters()

        if not self.inputXML and self.batchLength is not None:
            batchLength = seiscomp.core.TimeSpan(self.batchLength)
        else:
            batchLength = self.endTime - self.startTime

        batchStartTime = self.startTime
        while batchStartTime < self.endTime:
            if self.isExitRequested():
                return False

            batchEndTime = batchStartTime + batchLength
            if batchEndTime > self.endTime:
                batchEndTime = self.endTime
            batchObjects = self._loadInputObjects(batchStartTime, batchEndTime)
            batchStartTime += batchLength

            if self.processingMode == "playback":
                # Sort objects by either creation or pick time
                objectTime = scocto.util.pickTime if self.usePickTime else scocto.util.creationTime
                batchObjects.sort(key=lambda obj: objectTime(obj))

                for obj in batchObjects:
                    seiscomp.logging.debug(obj.publicID())

                for obj in batchObjects:
                    if scocto.util.isPick(obj):
                        self._addPick(obj)

            elif self.processingMode == "offline":
                self.associator.feed(batchObjects)

            if self.processingMode in ("offline", "dump picks"):
                # Add all input objects to output.
                for obj in batchObjects:
                    self.ep.add(obj)


        if self.processingMode == "playback":
            # Process any remaining picks
            self._processPickQueue()

            for pickID in self.playbackAssociatedPicks:
                pick = self.playbackAssociatedPicks[pickID]
                self.ep.add(pick)

        elif self.processingMode == "offline":
            origins = self.associator.process()

            self.relocateOrigins(origins)


            ep = self.ep
            for origin in origins:
                ep.add(origin)

        elif self.processingMode == "dump picks":
            pass
        else:
            seiscomp.logging.error("Wrong processing mode " + self.processingMode)
            return False

        # Final output
        seiscomp.logging.debug("Writing output to %s" % self.outputXML)
        ar = seiscomp.io.XMLArchive()
        ar.setFormattedOutput(True)
        ar.create(self.outputXML)
        ar.writeObject(self.ep)
        ar.close()
        return True

    def _checkPickAuthor(self, pick):
        """
        Author check against the specified list of pick authors.

        If this list was not specified, all picks with pass this check.
        """
        if not self.pickAuthors:
            return True
        pickAuthor = pick.creationInfo().author()
        if pickAuthor in self.pickAuthors:
            return True
        msg = "pick %s %s -> stop" % (
            pick.publicID(),
            "author '%s' not in %s" % (pickAuthor, str(self.pickAuthors))
            )
        return False

    def _checkStation(self, pick):
        """
        Station whitelist match
        """
        if not self.whitelist:
            return True

        matches = self.whitelist.matches(pick.waveformID())
        ##msg = "pick " + pick.publicID()
        ##if matches:
        ##else:
        return matches

    def _checkPick(self, pick):
        if not self._checkStation(pick):
            return False

        if not self._checkPickAuthor(pick):
            return False

        if not self.associator.accepts(pick):
            return False

        return True

    def _storePick(self, pick):
        self.picks[pick.publicID()] = pick
        self.sortedPicks.append(pick)
        objectTime = scocto.util.pickTime
        self.sortedPicks.sort(key=lambda x: objectTime(x))
        self.pickQueue.append(pick)

        if self.processingMode == "playback":
            self._updatePlaybackTime(pick)
        return True

    def _updatePlaybackTime(self, obj):
        """
        Update the playback time from the creation time of the object.
        """
        if not self.processingMode == "playback":
            raise RuntimeError()
        creationTime = scocto.util.creationTime(obj)
        if self.playbackTime is None:
            self.playbackTime = creationTime
        else:
            self.playbackTime = max(self.playbackTime, creationTime)
        tstr = scocto.util.time2str(self.playbackTime)
        seiscomp.logging.debug("Playback time is now %s" % (tstr,))

    def now(self):
        if self.processingMode == "online":
            return seiscomp.core.Time.UTC()
        elif self.playbackTime is not None:
            return self.playbackTime
        else:
            return seiscomp.core.Time.UTC()


    def _due(self, pick, now=None):
        if not now:
            now = self.now()
        getTime = scocto.util.pickTime if self.usePickTime else scocto.util.creationTime
        if float(now - getTime(pick)) >= self.config.pick_delay:
            return True
        return False

    def _processPickQueue(self):
        now = self.now()
        processedPicks = list()
        for pick in self.pickQueue:
            if self._due(pick):
                self._processPick(pick)
                processedPicks.append(pick)
        for pick in processedPicks:
            self.pickQueue.remove(pick)

        return True

    def _processPick(self, new_pick):
        if self.isExitRequested():
            return

        seiscomp.logging.info("Processing pick " + new_pick.publicID())
        if self.processingMode == "playback":
            seiscomp.logging.debug("Playback time is " + scocto.util.time2str(self.now()))

        # Process pick in the context of other picks within a small time window
        dt = seiscomp.core.TimeSpan(120 + self.config.pick_delay)
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

        if len(picks) < self.config.pyocto.n_p_picks:
            return

        seiscomp.logging.debug("Feeding associator with %d picks" % len(picks))

        self.associator.clear()
        self.associator.feed(picks)
        origins = self.associator.process()
        if not origins:
            return

        filtered_origins = []

        for origin in origins:
            msg = "candidate origin " + origin.publicID() + (" with %d arrivals" % origin.arrivalCount())
            seiscomp.logging.debug(msg)

            if not scocto.util.originReferencesPick(origin, new_pick):
                msg = "candidate origin " + origin.publicID() + \
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

            matching_event = self.eventList.find_matching_event(origin)

            method = origin.methodID()

            if matching_event:
                seiscomp.logging.debug("matching event found")
                last = matching_event.origins[method][-1]
                if scocto.util.compareOrigins(last, origin) > 0:
                    seiscomp.logging.debug("improvement: %d -> %d" % (last.arrivalCount(), origin.arrivalCount()))
                else:
                    seiscomp.logging.debug("no improvement - skipping origin")
                    discarded_origins.append(origin)
                    continue
            else:
                seiscomp.logging.debug("new event")
                seiscomp.logging.debug("improvement: %d -> %d" % (0, origin.arrivalCount()))
                matching_event = scocto.event.MyEvent()
                self.eventList.append(matching_event)

            matching_event.set_origin(origin, self.picks)

            if matching_event.lastPublished:
                pass

        for discarded_origin in discarded_origins:
            origins.remove(discarded_origin)

        for origin in origins:
            self.populateMetaData(origin)

        if self.processingMode == "online":
            # prepare notifier message
            ep = seiscomp.datamodel.EventParameters()
            seiscomp.datamodel.Notifier.Enable()
            for origin in origins:
                ep.add(origin)
            msg = seiscomp.datamodel.Notifier.GetMessage()
            seiscomp.datamodel.Notifier.Disable()
            if self.commandline().hasOption("test"):
                for origin in origins:
                    seiscomp.logging.info("test mode - not sending " + origin.publicID())
            else:
                if self.connection().send(msg):
                    for origin in origins:
                        seiscomp.logging.info("sent " + origin.publicID())
                else:
                    for origin in origins:
                        seiscomp.logging.info("failed to send " + origin.publicID())
        else:
            # offline/playback mode
            for origin in origins:
                self.ep.add(origin)

                for i in range(origin.arrivalCount()):
                    arrival = origin.arrival(i)
                    pick = seiscomp.datamodel.Pick.Find(arrival.pickID())
                    self.playbackAssociatedPicks[arrival.pickID()] = pick

        return True

    def _addPick(self, pick):
        """
        Feed a new pick to the processing

        Called in online and playback mode.
        """
        if not self._checkPick(pick):
            return

        if not self._storePick(pick):
            return

        if not self._processPickQueue():
            return

    def addObject(self, parentID, obj):
        """
        Add a new object just received from the messaging
        """
        if self.isExitRequested():
            return

        types = [ seiscomp.datamodel.Pick ]
        obj = anycast(obj, types)

        if type(obj) is seiscomp.datamodel.Pick:
            return self._addPick(obj)

    def cleanup(self):
        pass

    def handleTimeout(self):
        self._processPickQueue()
        self.cleanup()


def main():
    app = App(len(sys.argv), sys.argv)
    return app()


if __name__ == "__main__":
    main()
