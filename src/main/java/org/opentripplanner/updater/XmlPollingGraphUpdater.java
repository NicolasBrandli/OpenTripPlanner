package org.opentripplanner.updater;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opentripplanner.common.model.T2;
import org.opentripplanner.routing.alertpatch.Alert;
import org.opentripplanner.routing.alertpatch.AlertPatch;
import org.opentripplanner.routing.alertpatch.StreetPatch;
import org.opentripplanner.routing.graph.Edge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.impl.AlertPatchServiceImpl;
import org.opentripplanner.routing.impl.StreetPatchServiceImpl;
import org.opentripplanner.routing.services.AlertPatchService;
import org.opentripplanner.routing.services.StreetPatchService;
import org.opentripplanner.routing.services.notes.DynamicStreetNotesSource;
import org.opentripplanner.routing.services.notes.MatcherAndAlert;
import org.opentripplanner.routing.services.notes.NoteMatcher;
import org.opentripplanner.routing.services.notes.StreetNotesService;
import org.opentripplanner.util.xml.XmlDataListDownloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.vividsolutions.jts.geom.Point;

/**
 * A graph updater that reads xml data and updates a DynamicStreetNotesSource or alertPatchService.
 * Useful when reading xml data from external sources, which are not based on OSM and where data has
 * to be matched to the street network or simply to add alertPatches on routes.
 * 
 * Classes that extend this class should provide getNote which parses the features into notes.
 * Also the implementing classes should be added to the GraphUpdaterConfigurator
 * 
 * @see XmlPollingGraphUpdater
 * 
 * @author brandlin
 */
public abstract class XmlPollingGraphUpdater extends PollingGraphUpdater {
    private static final String UPDATE_TYPE_ROUTE = "route";

    private static final String UPDATE_TYPE_STREET = "street";

    protected Graph graph;

    private GraphUpdaterManager updaterManager;

    private SetMultimap<Edge, MatcherAndAlert> notesForEdge;

    /**
     * Set of unique matchers, kept during building phase, used for interning (lots of note/matchers
     * are identical).
     */
    private Map<T2<NoteMatcher, Alert>, MatcherAndAlert> uniqueMatchers;

    private URL url;

    private String path;

    private String updateType;

    List<StreetPatch> patches = new ArrayList<StreetPatch>();

    private XmlDataListDownloader<StreetPatch> xmlFeatureDownloader;

    private XmlDataListDownloader<AlertPatch> xmlAlertDownloader;

    protected static final SimpleFeatureType pointType = makePointType();

    static SimpleFeatureType makePointType() {
        SimpleFeatureTypeBuilder tbuilder = new SimpleFeatureTypeBuilder();
        tbuilder.setName("points");
        tbuilder.setCRS(DefaultGeographicCRS.WGS84);
        tbuilder.add("Geometry", Point.class);
        tbuilder.add("Time", Integer.class);
        return tbuilder.buildFeatureType();
    }

    private DynamicStreetNotesSource notesSource = new DynamicStreetNotesSource();

    private AlertPatchService alertPatchService;
    
    private StreetPatchService streetPatchService;

    private Set<String> patchIds = new HashSet<String>();

    // Set the matcher type for the notes, can be overridden in extending classes
    private static final NoteMatcher NOTE_MATCHER = StreetNotesService.DRIVING_MATCHER;

    private static Logger LOG = LoggerFactory.getLogger(XmlPollingGraphUpdater.class);

    /**
     * Here the updater can be configured using the properties in the file 'Graph.properties'. The
     * property frequencySec is already read and used by the abstract base class.
     */
    @Override
    protected void configurePolling(Graph graph, JsonNode config) throws Exception {
        url = new URL(config.path("url").asText());
        path = config.path("path").asText();
        updateType = config.path("updateType").asText();
        this.graph = graph;
        AlertPatchService alertPatchService = new AlertPatchServiceImpl(graph);
        this.alertPatchService = alertPatchService;
        StreetPatchService streetPatchService = new StreetPatchServiceImpl(graph);
        this.streetPatchService = streetPatchService;
        
        LOG.info("Configured xml polling updater: frequencySec={}, url={}", frequencySec,
                url.toString());
    }

    /**
     * Here the updater gets to know its parent manager to execute GraphWriterRunnables.
     */
    @Override
    public void setGraphUpdaterManager(GraphUpdaterManager updaterManager) {
        this.updaterManager = updaterManager;
    }

    /**
     * Setup the data source and add the DynamicStreetNotesSource to the graph
     */
    @Override
    public void setup() throws IOException, FactoryException {
        LOG.info("Setup xml polling updater");

        if (UPDATE_TYPE_STREET.equals(updateType)) {
            xmlFeatureDownloader = new XmlDataListDownloader<StreetPatch>();
            xmlFeatureDownloader.setPath(path);
            xmlFeatureDownloader
                    .setDataFactory(new XmlDataListDownloader.XmlDataFactory<StreetPatch>() {
                        @Override
                        public StreetPatch build(Map<String, String> attributes) {
                            return makeStreetPatch(attributes);
                        }
                    });
        } else if (UPDATE_TYPE_ROUTE.equals(updateType)) {
            xmlAlertDownloader = new XmlDataListDownloader<AlertPatch>();
            xmlAlertDownloader.setPath(path);
            xmlAlertDownloader
                    .setDataFactory(new XmlDataListDownloader.XmlDataFactory<AlertPatch>() {
                        @Override
                        public AlertPatch build(Map<String, String> attributes) {
                            return makeAlertPatch(attributes);
                        }
                    });
        }

        graph.streetNotesService.addNotesSource(notesSource);
    }

    @Override
    public void teardown() {
        LOG.info("Teardown xml polling updater");
    }

    /**
     * The function is run periodically by the update manager. The extending class should provide
     * the getNote method. It is not implemented here as the requirements for different updaters can
     * be vastly different dependent on the data source.
     */
    @Override
    protected void runPolling() throws IOException {
        LOG.info("Run xml polling updater with hashcode: {}", this.hashCode());

        notesForEdge = HashMultimap.create();
        uniqueMatchers = new HashMap<>();

        if (UPDATE_TYPE_STREET.equals(updateType)) {
            runStreetPolling();
        } else if (UPDATE_TYPE_ROUTE.equals(updateType)) {
            runRoutePolling();
        }
    }

    private void runStreetPolling() throws IOException {
        streetPatchService.expire(patchIds);
        patchIds.clear();
        
        List<StreetPatch> patchesList = xmlFeatureDownloader.download(url.toString(), false);
        LOG.info("found {} features", patchesList.size());

        Iterator<StreetPatch> patches = patchesList.iterator();

        while (patches.hasNext()) {
            StreetPatch patch = patches.next();
            if (patch.getLocation() == null)
                continue;

            if (patch.getAlert() == null)
                continue;

            streetPatchService.matchToStreet(patch);

            if (patch.getEdge() != null) {
                patchIds.add(patch.getId());
                streetPatchService.apply(patch);
                //addNote(edges.best.edge, alert, NOTE_MATCHER);
                LOG.info("{} matched to : {}", patch.getAlert().alertHeaderText.getSomeTranslation(),
                        patch.getEdge().toString());
            } else {
                LOG.info("{} matched to nothing", patch.getAlert().alertHeaderText.getSomeTranslation());
            }
        }
        updaterManager.execute(new XmlGraphWriter());
    }

    private void runRoutePolling() throws IOException {
        alertPatchService.expire(patchIds);
        patchIds.clear();

        List<AlertPatch> alertsList = xmlAlertDownloader.download(url.toString(), false);
        LOG.info("found {} features", alertsList.size());

        Iterator<AlertPatch> alerts = alertsList.iterator();

        while (alerts.hasNext()) {
            AlertPatch alert = alerts.next();

            if (alert == null)
                continue;

            patchIds.add(alert.getId());
            alertPatchService.apply(alert);

            LOG.info("Alert : {} : {}", alert.getId(),
                    alert.getAlert().alertHeaderText.getSomeTranslation());
        }
        updaterManager.execute(new XmlGraphWriter());
    }

    /**
     * Parses a SimpleFeature and returns an Alert if the feature should create one.
     */
    //protected abstract Alert getNote(SimpleFeature feature);

    protected abstract StreetPatch makeStreetPatch(Map<String, String> attributes);

    protected abstract AlertPatch makeAlertPatch(Map<String, String> attributes);

    /**
     * Changes the note source to use the newly generated notes
     */
    private class XmlGraphWriter implements GraphWriterRunnable {
        public void run(Graph graph) {
            //notesSource.setNotes(notesForEdge);
        }
    }

    /**
     * Methods for writing into notesForEdge TODO: Should these be extracted into somewhere?
     */
    private void addNote(Edge edge, Alert note, NoteMatcher matcher) {
        if (LOG.isDebugEnabled())
            LOG.debug("Adding note {} to {} with matcher {}", note, edge, matcher);
        notesForEdge.put(edge, buildMatcherAndAlert(matcher, note));
    }

    /**
     * Create a MatcherAndAlert, interning it if the note and matcher pair is already created. Note:
     * we use the default Object.equals() for matchers, as they are mostly already singleton
     * instances.
     */
    private MatcherAndAlert buildMatcherAndAlert(NoteMatcher noteMatcher, Alert note) {
        T2<NoteMatcher, Alert> key = new T2<>(noteMatcher, note);
        MatcherAndAlert interned = uniqueMatchers.get(key);
        if (interned != null) {
            return interned;
        }
        MatcherAndAlert ret = new MatcherAndAlert(noteMatcher, note);
        uniqueMatchers.put(key, ret);
        return ret;
    }

    public Collection<HashMap<String, Object>> getUpdates() {
        Collection<HashMap<String, Object>> res = new ArrayList<HashMap<String, Object>>();
        if (UPDATE_TYPE_STREET.equals(updateType)) {
            HashMap<String, Object> streetPatches = new HashMap<String, Object>();
            streetPatches.put("streetPatches", streetPatchService.getAllStreetPatches());
            res.add(streetPatches);
        } else if (UPDATE_TYPE_ROUTE.equals(updateType)) {
            HashMap<String, Object> alerts = new HashMap<String, Object>();
            alerts.put("alerts", alertPatchService.getAllAlertPatches());
            res.add(alerts);
        }
        return res;
    }

}
