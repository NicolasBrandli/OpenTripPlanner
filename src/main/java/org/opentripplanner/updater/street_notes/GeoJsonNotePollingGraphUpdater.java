package org.opentripplanner.updater.street_notes;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.GeoJsonObject;
import org.opengis.referencing.FactoryException;
import org.opentripplanner.analyst.UnsupportedGeometryException;
import org.opentripplanner.common.geometry.GeometryUtils;
import org.opentripplanner.common.geometry.SphericalDistanceLibrary;
import org.opentripplanner.common.model.T2;
import org.opentripplanner.routing.alertpatch.Alert;
import org.opentripplanner.routing.edgetype.StreetEdge;
import org.opentripplanner.routing.graph.Edge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.services.notes.DynamicStreetNotesSource;
import org.opentripplanner.routing.services.notes.MatcherAndAlert;
import org.opentripplanner.routing.services.notes.NoteMatcher;
import org.opentripplanner.routing.services.notes.StreetNotesService;
import org.opentripplanner.updater.GraphUpdaterManager;
import org.opentripplanner.updater.GraphWriterRunnable;
import org.opentripplanner.updater.PollingGraphUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.vividsolutions.jts.geom.Geometry;

/**
 * A graph updater that reads a Geojson and updates a DynamicStreetNotesSource.
 * Useful when reading geodata from legacy/external sources, which are not based on OSM
 * and where data has to be matched to the street network.
 *
 * Classes that extend this class should provide getNote which parses the GeoJson features
 * into notes. Also the implementing classes should be added to the GraphUpdaterConfigurator
 *
 * @see GAMPollingGraphUpdater
 *
 * @author NicolasBrandli
 */
public abstract class GeoJsonNotePollingGraphUpdater extends PollingGraphUpdater {
    protected Graph graph;

    private GraphUpdaterManager updaterManager;

    private SetMultimap<Edge, MatcherAndAlert> notesForEdge;

    /**
     * Set of unique matchers, kept during building phase, used for interning (lots of note/matchers
     * are identical).
     */
    private Map<T2<NoteMatcher, Alert>, MatcherAndAlert> uniqueMatchers;

    private URL url;
    private ObjectMapper mapper;
    
    private DynamicStreetNotesSource notesSource = new DynamicStreetNotesSource();

    // How much should the geometries be padded with in order to be sure they intersect with graph edges
    private static final double SEARCH_RADIUS_M = 2;
    private static final double SEARCH_RADIUS_DEG = SphericalDistanceLibrary.metersToDegrees(SEARCH_RADIUS_M);

    // Set the matcher type for the notes, can be overridden in extending classes
    private static final NoteMatcher NOTE_MATCHER = StreetNotesService.ALWAYS_MATCHER;

    private static Logger LOG = LoggerFactory.getLogger(GeoJsonNotePollingGraphUpdater.class);

    /**
     * Here the updater can be configured using the properties in the file 'router-config.json'.
     * The property frequencySec is already read and used by the abstract base class.
     */
    @Override
    protected void configurePolling(Graph graph, JsonNode config) throws Exception {
        url = new URL(config.path("url").asText());
        this.graph = graph;
        LOG.info("Configured GeoJson polling updater: frequencySec={}, url={}",
                frequencySec, url.toString());
    }

    /**
     * Here the updater gets to know its parent manager to execute GraphWriterRunnables.
     */
    @Override
    public void setGraphUpdaterManager(GraphUpdaterManager updaterManager) {
        this.updaterManager = updaterManager;
    }

    /**
     * Add the DynamicStreetNotesSource to the graph
     */
    @Override
    public void setup() throws IOException, FactoryException {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        graph.streetNotesService.addNotesSource(notesSource);
    }

    @Override
    public void teardown() {
        LOG.info("Teardown GeoJson polling updater");
    }

    /**
     * The function is run periodically by the update manager.
     * The extending class should provide the getNote method. It is not implemented here
     * as the requirements for different updaters can be vastly different dependent on the data source.
     */
    @Override
    protected void runPolling() throws IOException{
        LOG.info("Run GeoJson polling updater with hashcode: {}", this.hashCode());

        notesForEdge = HashMultimap.create();
        uniqueMatchers = new HashMap<>();

        URLConnection conn = url.openConnection();
        InputStream in = conn.getInputStream();
        FeatureCollection featureCollection = mapper.readValue(in, FeatureCollection.class);
        in.close();

        for (Feature feature : featureCollection.getFeatures()) {
            GeoJsonObject featureGeom = feature.getGeometry();
            if (featureGeom == null) continue;

            try {
                Geometry geom = GeometryUtils.convertGeoJsonToJtsGeometry(featureGeom);
                
                Alert alert = getNote(feature);
                if (alert == null) continue;

                Geometry searchArea = geom.buffer(SEARCH_RADIUS_DEG);
                Collection<Edge> edges = graph.streetIndex.getEdgesForEnvelope(searchArea.getEnvelopeInternal());
                for(Edge edge: edges){
                    if (edge instanceof StreetEdge && !searchArea.disjoint(edge.getGeometry())) {
                        addNote(edge, alert, NOTE_MATCHER);
                    }
                }

            } catch (UnsupportedGeometryException e) {
                LOG.warn("Unsupported Geometry Type in GeoJson polling updater with hashcode: {}", this.hashCode());
            }
        }
        updaterManager.execute(new GeoJsonGraphWriter());
    }

    /**
     * Parses a Feature and returns an Alert if the feature should create one.
     * The alert should be based on the fields specific for the specific GeoJson feed.
     */
    protected abstract Alert getNote(Feature feature);

    /**
     * Changes the note source to use the newly generated notes
     */
    private class GeoJsonGraphWriter implements GraphWriterRunnable {
        public void run(Graph graph) {
            notesSource.setNotes(notesForEdge);
        }
    }

    /**
     * Methods for writing into notesForEdge
     * TODO: Should these be extracted into somewhere?
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

}
