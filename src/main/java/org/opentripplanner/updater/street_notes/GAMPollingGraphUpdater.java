package org.opentripplanner.updater.street_notes;


import java.util.Date;

import org.geojson.Feature;
import org.opentripplanner.routing.alertpatch.Alert;
import org.opentripplanner.util.NonLocalizedString;

/**
 * Example implementation of a GeoJson based street note updater, which can be used to retrieve roadworks and other
 * temporary obstacles from a Geojson provided by the Grenoble-Alpes Metropole's Métromobilité website.
 *
 * Usage example:
 *
 * <pre>
 * {
 *      type: "GAM-street-note-polling-updater",
 *      frequencySec: 60,
 *      url: "http://data.metromobilite.fr/dyn/evtTR/geojson"
 * }
 * </pre>
 */


public class GAMPollingGraphUpdater extends GeoJsonNotePollingGraphUpdater {
    protected Alert getNote(Feature feature) {
        Alert alert = Alert.createSimpleAlerts(feature.getProperty("type"));
        alert.alertDescriptionText = feature.getProperty("text") == null ?
                new NonLocalizedString("") : new NonLocalizedString(feature.getProperty("text"));
        alert.effectiveStartDate = new Date( (long)feature.getProperty("startDate") );
        return alert;
    }
}