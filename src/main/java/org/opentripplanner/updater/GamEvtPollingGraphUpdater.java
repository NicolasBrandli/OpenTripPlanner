package org.opentripplanner.updater;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.opengis.feature.simple.SimpleFeature;
import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.alertpatch.Alert;
import org.opentripplanner.routing.alertpatch.AlertPatch;
import org.opentripplanner.routing.alertpatch.StreetPatch;
import org.opentripplanner.routing.alertpatch.TimePeriod;
import org.opentripplanner.routing.alertpatch.TranslatedString;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * Example implementation of a Xml based street note updater, which can be used to retrieve
 * roadworks and other temporary obstacles from xml data provided by the Grenoble Aples Metropole.
 * 
 * Usage example:
 * 
 * <pre>
 *        {
 *            type: "xml-polling-updater",
 *            frequencySec: 60,
 *            url: "http://domain.dom/streetEvts.xml",
 *            updateType: "street",
 *            path: "/Evts/Evt"
 *        },
 *        {
 *            type: "xml-polling-updater",
 *            frequencySec: 60,
 *            url: "http://domain.dom/routeEvts.xml",
 *            updateType: "route",
 *            path: "/Evts/Evt"
 *        }
 * </pre>
 */

public class GamEvtPollingGraphUpdater extends XmlPollingGraphUpdater {

    protected StreetPatch makeStreetPatch(Map<String, String> attributes) {

        StreetPatch patch = new StreetPatch();
        patch.setId(attributes.get("Code"));
        GenericLocation location = new GenericLocation(new Coordinate(Double.parseDouble(attributes.get("Lon")), Double.parseDouble(attributes.get("Lat"))));
        patch.setLocation(location);
        Alert alert = Alert.createSimpleAlerts(attributes.get("Loc"));
        alert.alertDescriptionText = attributes.get("Comment") == null ? new TranslatedString("")
        : new TranslatedString(attributes.get("Comment").toString());        
        patch.setAlert(alert);
        SimpleDateFormat parserSDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Date startDate = new Date();
        Date endDate = new Date();
        try {
            startDate = parserSDF.parse(attributes.get("DDebut"));
            endDate = parserSDF.parse((String) attributes.get("DFin"));
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

        
        SimpleFeatureBuilder fbuilder = new SimpleFeatureBuilder(pointType);
        GeometryFactory gf = new GeometryFactory();
        fbuilder.add(gf.createPoint(new Coordinate(Double.parseDouble(attributes.get("Lon")),
                Double.parseDouble(attributes.get("Lat")))));
        fbuilder.add(attributes.get("Code"));
        fbuilder.featureUserData("Loc", attributes.get("Loc"));
        fbuilder.featureUserData("Comment", attributes.get("Comment"));
        fbuilder.featureUserData("DDebut", attributes.get("DDebut"));
        return patch;
    }

    @Override
    protected AlertPatch makeAlertPatch(Map<String, String> attributes) {

        Alert alert = Alert.createSimpleAlerts((String) attributes.get("Loc"));
        alert.alertDescriptionText = attributes.get("Comment") == null ? new TranslatedString("")
                : new TranslatedString(attributes.get("Comment").toString());
        SimpleDateFormat parserSDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Date startTime, endTime;
        try {
            startTime = parserSDF.parse((String) attributes.get("DDebut"));
            endTime = parserSDF.parse((String) attributes.get("DFin"));
        } catch (ParseException e) {
            e.printStackTrace();
            startTime = new Date();
            endTime = new Date();// ends now
        }
        alert.effectiveStartDate = startTime;
        String id = attributes.get("Code");
        if (attributes.get("PertLigne") == null)
            return null;
        String codeLigne = attributes.get("PertLigne").replace('\n', ' ').trim();// ugly ? Mmm...
                                                                                 // yes
        String agency = codeLigne.substring(0, 3);
        String route = codeLigne.substring(4);

        List<TimePeriod> periods = new ArrayList<TimePeriod>();
        TimePeriod t = new TimePeriod();
        t.startTime = startTime.getTime() / 1000;
        t.endTime = endTime.getTime() / 1000;
        periods.add(t);

        AlertPatch alertPatch = new AlertPatch();
        alertPatch.setAgencyId(agency);
        alertPatch.setId(id);
        alertPatch.setRoute(new AgencyAndId(agency, route));

        alertPatch.setTimePeriods(periods);
        alertPatch.setAlert(alert);
        return alertPatch;
    }
}