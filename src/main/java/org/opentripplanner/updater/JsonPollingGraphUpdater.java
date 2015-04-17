package org.opentripplanner.updater;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
import org.opentripplanner.routing.alertpatch.StreetPatch;
import org.opentripplanner.routing.graph.Edge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.impl.StreetPatchServiceImpl;
import org.opentripplanner.routing.services.StreetPatchService;
import org.opentripplanner.routing.services.notes.DynamicStreetNotesSource;
import org.opentripplanner.routing.services.notes.MatcherAndAlert;
import org.opentripplanner.routing.services.notes.NoteMatcher;
import org.opentripplanner.routing.services.notes.StreetNotesService;
import org.opentripplanner.util.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.SetMultimap;
import com.vividsolutions.jts.geom.Point;

/**
 * A graph updater that reads json data and updates a DynamicStreetNotesSource or alertPatchService.
 * Useful when reading json data from external sources, which are not based on OSM and where data
 * has to be matched to the street network or simply to add alertPatches on routes.
 * 
 * Classes that extend this class should provide getNote which parses the features into notes. Also
 * the implementing classes should be added to the GraphUpdaterConfigurator
 * 
 * @see XmlPollingGraphUpdater
 * 
 * @author brandlin
 */
public abstract class JsonPollingGraphUpdater extends PollingGraphUpdater {
    private static final String UPDATE_TYPE_SPEED = "speed";

    private static final String UPDATE_TYPE_WHEIGHT = "wheight";

    protected Graph graph;

    private GraphUpdaterManager updaterManager;

    private SetMultimap<Edge, MatcherAndAlert> notesForEdge;

    /**
     * Set of unique matchers, kept during building phase, used for interning (lots of note/matchers
     * are identical).
     */
    private Map<T2<NoteMatcher, Alert>, MatcherAndAlert> uniqueMatchers;

    private String url;

    private String path;

    private String updateType;

    List<SimpleFeature> features = new ArrayList<SimpleFeature>();

    private StreetPatchService streetPatchService;

    private Set<String> patchIds = new HashSet<String>();

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

    // Set the matcher type for the notes, can be overridden in extending classes
    private static final NoteMatcher NOTE_MATCHER = StreetNotesService.DRIVING_MATCHER;

    private static Logger LOG = LoggerFactory.getLogger(JsonPollingGraphUpdater.class);

    /**
     * Here the updater can be configured using the properties in the file 'Graph.properties'. The
     * property frequencySec is already read and used by the abstract base class.
     */
    @Override
    protected void configurePolling(Graph graph, JsonNode config) throws Exception {
        url = config.path("url").asText();
        path = config.path("path").asText();
        updateType = config.path("updateType").asText();
        this.graph = graph;
        StreetPatchService streetPatchService = new StreetPatchServiceImpl(graph);
        this.streetPatchService = streetPatchService;

        LOG.info("Configured json polling updater: frequencySec={}, url={}", frequencySec,
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
        LOG.info("Setup json polling updater");

        /*
         * if (UPDATE_TYPE_SPEED.equals(updateType)) { xmlFeatureDownloader = new
         * XmlDataListDownloader<SimpleFeature>(); xmlFeatureDownloader.setPath(path);
         * xmlFeatureDownloader .setDataFactory(new
         * XmlDataListDownloader.XmlDataFactory<SimpleFeature>() {
         * 
         * @Override public SimpleFeature build(Map<String, String> attributes) { return
         * makeFeature(attributes); } }); } else if (UPDATE_TYPE_WHEIGHT.equals(updateType)) {
         * xmlAlertDownloader = new XmlDataListDownloader<AlertPatch>();
         * xmlAlertDownloader.setPath(path); xmlAlertDownloader .setDataFactory(new
         * XmlDataListDownloader.XmlDataFactory<AlertPatch>() {
         * 
         * @Override public AlertPatch build(Map<String, String> attributes) { return
         * makeAlertPatch(attributes); } }); }
         */

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
        LOG.info("Run json polling updater with hashcode: {}", this.hashCode());

        streetPatchService.expire(patchIds);
        patchIds.clear();

        /*
         * notesForEdge = HashMultimap.create(); uniqueMatchers = new HashMap<>();
         * 
         * if (UPDATE_TYPE_SPEED.equals(updateType)) { runSpeedPolling(); } else if
         * (UPDATE_TYPE_WHEIGHT.equals(updateType)) { //runRoutePolling(); }
         */
        try {
            InputStream data = HttpUtils.getData(url);
            if (data == null) {
                LOG.warn("Failed to get data from url " + url);
            }
            parseJSON(data);
            data.close();
        } catch (JsonProcessingException e) {
            LOG.warn("Error parsing bike rental feed from " + url + "(bad JSON of some sort)", e);
        } catch (IOException e) {
            LOG.warn("Error reading bike rental feed from " + url, e);
        }
    }

    private void parseJSON(InputStream dataStream) throws JsonProcessingException,
            IllegalArgumentException, IOException {

        // ArrayList<BikeRentalStation> out = new ArrayList<BikeRentalStation>();

        String carsString = convertStreamToString(dataStream);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(carsString);

        /*
         * if (!path.equals("")) { String delimiter = "/"; String[] parseElement =
         * path.split(delimiter); for(int i =0; i < parseElement.length ; i++) { rootNode =
         * rootNode.path(parseElement[i]); }
         * 
         * if (rootNode.isMissingNode()) { throw new
         * IllegalArgumentException("Could not find jSON elements " + path); } }
         */
        rootNode = rootNode.path("features");
        for (int i = 0; i < rootNode.size(); i++) {
            JsonNode node = rootNode.get(i);
            if (node == null) {
                continue;
            }

            StreetPatch streetPatch = makeStreetPatch(node);
            if (streetPatch == null)
                continue;
            if (streetPatch.getLocation() == null)
                continue;
            if (streetPatch.getAlert() == null)
                continue;

            streetPatchService.matchToStreet(streetPatch);

            if (streetPatch.getEdge() != null) {
                // addNote(edges.best.edge, alert, NOTE_MATCHER);
                // graph.addStreetPatch(edges.best.edge, streetPatch);

                patchIds.add(streetPatch.getId());
                streetPatchService.apply(streetPatch);

                LOG.info("{} : {} matched to : {}", streetPatch.getId(),
                        streetPatch.getAlert().alertHeaderText.getSomeTranslation(), streetPatch
                                .getEdge().toString());
            } else {
                LOG.info("{} : {} matched to nothing", streetPatch.getId(),
                        streetPatch.getAlert().alertHeaderText.getSomeTranslation());
            }
        }
        updaterManager.execute(new JsonGraphWriter());
    }

    private String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner scanner = null;
        String result = "";
        try {

            scanner = new java.util.Scanner(is).useDelimiter("\\A");
            result = scanner.hasNext() ? scanner.next() : "";
            scanner.close();
        } finally {
            if (scanner != null)
                scanner.close();
        }
        return result;

    }

    /*
     * private void runStreetPolling() throws IOException { List<SimpleFeature> featuresList =
     * xmlFeatureDownloader.download(url.toString(), false); LOG.info("found {} features",
     * featuresList.size());
     * 
     * Iterator<SimpleFeature> features = featuresList.iterator();
     * 
     * while (features.hasNext()) { SimpleFeature feature = features.next(); if
     * (feature.getDefaultGeometry() == null) continue;
     * 
     * Alert alert = getNote(feature); if (alert == null) continue;
     * 
     * Geometry geom = (Geometry) feature.getDefaultGeometry(); GenericLocation location = new
     * GenericLocation(geom.getCoordinate());
     * 
     * TraversalRequirements reqs = new TraversalRequirements(); reqs.modes.setCar(true);
     * 
     * CandidateEdgeBundle edges = graph.streetIndex.getClosestEdges(location, reqs, null, null,
     * false);
     * 
     * // Sort them by score. CandidateEdgeScoreComparator comp = new
     * CandidateEdgeScoreComparator(); Collections.sort(edges, comp);
     * 
     * if (!edges.isEmpty()) { addNote(edges.best.edge, alert, NOTE_MATCHER);
     * LOG.info("{} matched to : {}", alert.alertHeaderText.getSomeTranslation(),
     * edges.best.edge.toString()); } else { LOG.info("{} matched to nothing",
     * alert.alertHeaderText.getSomeTranslation()); } } updaterManager.execute(new
     * XmlGraphWriter()); }
     */

    /*
     * private void runRoutePolling() throws IOException {
     * 
     * 
     * List<AlertPatch> alertsList = xmlAlertDownloader.download(url.toString(), false);
     * LOG.info("found {} features", alertsList.size());
     * 
     * 
     * 
     * LOG.info("Alert : {} : {}", alert.getId(),
     * alert.getAlert().alertHeaderText.getSomeTranslation()); } updaterManager.execute(new
     * XmlGraphWriter()); }
     */

    protected abstract StreetPatch makeStreetPatch(JsonNode node);

    /**
     * Changes the note source to use the newly generated notes
     */
    private class JsonGraphWriter implements GraphWriterRunnable {
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

        HashMap<String, Object> streetPatches = new HashMap<String, Object>();
        streetPatches.put("streetPatches", streetPatchService.getAllStreetPatches());
        res.add(streetPatches);

        return res;
    }

}
