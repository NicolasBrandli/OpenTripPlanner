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

package org.opentripplanner.routing.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.opentripplanner.common.model.GenericLocation;
import org.opentripplanner.routing.alertpatch.StreetPatch;
import org.opentripplanner.routing.core.TraversalRequirements;
import org.opentripplanner.routing.edgetype.StreetEdge;
import org.opentripplanner.routing.graph.Graph;
import org.opentripplanner.routing.impl.CandidateEdge.CandidateEdgeScoreComparator;
import org.opentripplanner.routing.services.StreetPatchService;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;

public class StreetPatchServiceImpl implements StreetPatchService {

    private Graph graph;

    private Map<String, StreetPatch> streetPatches = new HashMap<String, StreetPatch>();

    private ListMultimap<StreetEdge, StreetPatch> patchesByEdge = LinkedListMultimap.create();

    public StreetPatchServiceImpl(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Collection<StreetPatch> getAllStreetPatches() {
        return streetPatches.values();
    }

    @Override
    public Collection<StreetPatch> getStreetPatches(StreetEdge edge) {
        List<StreetPatch> result = patchesByEdge.get(edge);
        if (result == null) {
            result = Collections.emptyList();
        }
        return result;
    }

    @Override
    public synchronized void apply(StreetPatch streetPatch) {
        if (streetPatches.containsKey(streetPatch.getId())) {
            expire(streetPatches.get(streetPatch.getId()));
        }

        streetPatch.apply(graph);
        streetPatches.put(streetPatch.getId(), streetPatch);

        StreetEdge edge = streetPatch.getEdge();
        if (edge != null) {
            patchesByEdge.put(edge, streetPatch);
        }
    }

    @Override
    public void expire(Set<String> purge) {
        for (String patchId : purge) {
            if (streetPatches.containsKey(patchId)) {
                expire(streetPatches.get(patchId));
            }
        }

        streetPatches.keySet().removeAll(purge);
    }

    @Override
    public void expireAll() {
        for (StreetPatch alertPatch : streetPatches.values()) {
            expire(alertPatch);
        }
        streetPatches.clear();
    }

    @Override
    public void expireAllExcept(Set<String> retain) {
        ArrayList<String> toRemove = new ArrayList<String>();

        for (Entry<String, StreetPatch> entry : streetPatches.entrySet()) {
            final String key = entry.getKey();
            if (!retain.contains(key)) {
                toRemove.add(key);
                expire(entry.getValue());
            }
        }
        streetPatches.keySet().removeAll(toRemove);
    }

    private void expire(StreetPatch streetPatch) {
        StreetEdge edge = streetPatch.getEdge();
        if (edge != null) {
            patchesByEdge.remove(edge, streetPatch);
        }
        streetPatch.remove(graph);
    }

    @Override
    public void matchToStreet(StreetPatch streetPatch) {
        StreetEdge edge = getStreetFromLocation(streetPatch.getLocation());
    
        if (edge!=null) {
            streetPatch.setEdge(edge);
        }
    }
    
    @Override
    public StreetEdge getStreetFromLocation(GenericLocation location) {
     
        TraversalRequirements reqs = new TraversalRequirements();
        reqs.modes.setCar(true);
    
        CandidateEdgeBundle edges = graph.streetIndex.getClosestEdges(
                location, reqs, null, null, false);
    
        // Sort them by score.
        CandidateEdgeScoreComparator comp = new CandidateEdgeScoreComparator();
        Collections.sort(edges, comp);
    
        if (!edges.isEmpty()) {
            return edges.best.edge;
        }
        return null;
    }
    
}
