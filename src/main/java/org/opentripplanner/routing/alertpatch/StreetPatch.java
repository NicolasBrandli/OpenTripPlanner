/* This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public License
 as published by the Free Software Foundation, either version 3 of
 the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package org.opentripplanner.routing.alertpatch;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.core.State;
import org.opentripplanner.routing.edgetype.StreetEdge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.util.PolylineEncoder;
import org.opentripplanner.util.model.EncodedPolylineBean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This adds a note to all boardings of a given route or stop (optionally, in a given direction)
 *
 * @author novalis
 *
 */
@XmlRootElement(name = "StreetPatch")
public class StreetPatch implements Serializable {
    private static final long serialVersionUID = 20140319L;

    private String id;

    private Alert alert;

    private List<TimePeriod> timePeriods = new ArrayList<TimePeriod>();

    private StreetEdge edge;
    
    private  GenericLocation location;
    
    /*
     * car speed overridden in meters per second
     */
    private float carSpeed = -1;

    /*
     * whether the patch is blocking or not
     */
    private boolean blocking = false;

    @XmlElement
    public Alert getAlert() {
        return alert;
    }

    public boolean displayDuring(State state) {
        for (TimePeriod timePeriod : timePeriods) {
            if (state.getTimeSeconds() >= timePeriod.startTime) {
                if (state.getStartTimeSeconds() < timePeriod.endTime) {
                    return true;
                }
            }
        }
        return false;
    }

    @XmlElement
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void apply(Graph graph) {
        graph.addStreetPatch(edge, this);
    }

    public void remove(Graph graph) {
        graph.removeStreetPatch(edge, this);
    }

    public void setAlert(Alert alert) {
        this.alert = alert;
    }

    private void writeObject(ObjectOutputStream os) throws IOException {
        if (timePeriods instanceof ArrayList<?>) {
            ((ArrayList<TimePeriod>) timePeriods).trimToSize();
        }
        os.defaultWriteObject();
    }

    public List<TimePeriod> getTimePeriods() {
        return timePeriods;
    }
    
    public void setTimePeriods(List<TimePeriod> periods) {
        timePeriods = periods;
    }
    
    @JsonIgnore
    public StreetEdge getEdge() {
        return edge;
    }
    
    @JsonProperty("edge")
    public String getEdgeGeometry() {
        EncodedPolylineBean geometry = PolylineEncoder.createEncodings(edge.getGeometry());        
        return geometry.getPoints();
    }

    public String getEdgeName() {
        return edge.getName();
    }

    public void setEdge(StreetEdge edge) {
        this.edge = edge;
    }
    
    @JsonProperty("lat")
    public Double getLat() {
        return location.lat;
    }
    
    @JsonProperty("lng")
    public Double getLng() {
        return location.lng;
    }
    
    @JsonIgnore
    public GenericLocation getLocation() {
        return location;
    }

    public void setLocation(GenericLocation location) {
        this.location = location;
    }

    public float getCarSpeed() {
        return carSpeed;
    }

    public void setCarSpeed(float carSpeed) {
        this.carSpeed = carSpeed;
    }

    public boolean getBlocking() {
        return blocking;
    }

    public void setBlocking(boolean blocking) {
        this.blocking = blocking;
    }
    public boolean equals(Object o) {
        if (!(o instanceof StreetPatch)) {
            return false;
        }
        StreetPatch other = (StreetPatch) o;
        if (edge == null) {
            if (other.edge != null) {
                return false;
            }
        } else {
            if (!edge.equals(other.edge)) {
                return false;
            }
        }
        if (location == null) {
            if (other.location != null) {
                return false;
            }
        } else {
            if (!location.equals(other.location)) {
                return false;
            }
        }
        if (alert == null) {
            if (other.alert != null) {
                return false;
            }
        } else {
            if (!alert.equals(other.alert)) {
                return false;
            }
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else {
            if (!id.equals(other.id)) {
                return false;
            }
        }
        if (timePeriods == null) {
            if (other.timePeriods != null) {
                return false;
            }
        } else {
            if (!timePeriods.equals(other.timePeriods)) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        return ((edge == null ? 0 : edge.hashCode()) +
                (location == null ? 0 : location.hashCode()) +
                (alert == null ? 0 : alert.hashCode()));
    }
}
