package org.opentripplanner.updater;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.alertpatch.Alert;
import org.opentripplanner.routing.alertpatch.StreetPatch;
import org.opentripplanner.routing.alertpatch.TimePeriod;

import com.fasterxml.jackson.databind.JsonNode;
import com.vividsolutions.jts.geom.Coordinate;

/**
 * Example implementation of a geojson based street note updater, which can be used to retrieve
 * roadworks and other temporary obstacles from geojson data provided by the Grenoble Aples
 * Metropole.
 * 
 * Usage example:
 * 
 * <pre>
 *        {
 *            type: "json-polling-updater",
 *            frequencySec: 60,
 *            url: "http://domain.dom/dynfile.geojson",
 *            updateType: "speed"
 *        }
 * </pre>
 */

public class GamCarsPollingGraphUpdater extends JsonPollingGraphUpdater {

    protected StreetPatch makeStreetPatch(JsonNode node) {

        JsonNode geomNode = node.path("geometry");
        JsonNode propNode = node.path("properties");
        if (propNode.get("V").asText() == "-1")
            return null;
        StreetPatch patch = new StreetPatch();
        patch.setId(propNode.get("CODE").asText());
        GenericLocation location = new GenericLocation(new Coordinate(geomNode.get("coordinates")
                .get(0).asDouble(), geomNode.get("coordinates").get(1).asDouble()));
        patch.setLocation(location);
        Alert alert = Alert.createSimpleAlerts(propNode.get("LIBELLE").asText());
        patch.setAlert(alert);
        SimpleDateFormat parserSDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Date startDate = new Date();
        Date endDate = new Date();
        try {
            startDate = parserSDF.parse((String) propNode.get("H").asText());
            endDate = new Date(startDate.getTime() + (1000 * 60 * 24));// 24 min
        } catch (ParseException e) {
            e.printStackTrace();
        }
        alert.effectiveStartDate = startDate;

        List<TimePeriod> periods = new ArrayList<TimePeriod>();
        TimePeriod t = new TimePeriod();
        t.startTime = startDate.getTime() / 1000;
        t.endTime = endDate.getTime() / 1000;
        periods.add(t);
        patch.setTimePeriods(periods);

        if ("GRE".equals(propNode.get("CODE").asText().substring(0, 3))) {
            return null;
        } else {
            patch.setCarSpeed((float) (propNode.get("V").floatValue()));// Already in m/s
        }
        /*
         * fbuilder.featureUserData("Q", propNode.get("Q")); fbuilder.featureUserData("T",
         * propNode.get("T"));
         */
        return patch;
    }
}