package org.opentripplanner.api.resource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.opentripplanner.standalone.OTPServer;
import org.opentripplanner.standalone.Router;
import org.opentripplanner.updater.GraphUpdater;
import org.opentripplanner.updater.GraphUpdaterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Report the status of the graph updaters via a web service.
 */
@Path("/routers/{routerId}/updaters")
@Produces(MediaType.APPLICATION_JSON)
public class UpdaterStatusResource {

    private static final Logger LOG = LoggerFactory.getLogger(UpdaterStatusResource.class);

    /** Choose short or long form of results. */
    @QueryParam("detail")
    private boolean detail = false;

    Router router;

    public UpdaterStatusResource(@Context OTPServer otpServer,
            @PathParam("routerId") String routerId) {
        router = otpServer.getRouter(routerId);
    }

    /** Return a list of all agencies in the graph. */
    @GET
    public Response getUpdaters() {
        GraphUpdaterManager updaterManager = router.graph.updaterManager;
        if (updaterManager == null) {
            return Response.status(Response.Status.NOT_FOUND).entity("No updaters running.")
                    .build();
        }
        return Response.status(Response.Status.OK).entity(updaterManager.getUpdaterDescriptions())
                .build();
    }

    /** Return status for a specific updater. */
    @GET
    @Path("/{updaterId}")
    public Response getUpdaters(@PathParam("updaterId") int updaterId) {
        GraphUpdaterManager updaterManager = router.graph.updaterManager;
        if (updaterManager == null) {
            return Response.status(Response.Status.NOT_FOUND).entity("No updaters running.")
                    .build();
        }
        GraphUpdater updater = updaterManager.getUpdater(updaterId);
        if (updater == null) {
            return Response.status(Response.Status.NOT_FOUND).entity("No updater with that ID.")
                    .build();
        }
        return Response.status(Response.Status.OK).entity(updater.getClass()).build();
    }

    /** Return the list of current updates for a specific updater. */
    @GET
    @Path("/{updaterId}/updates")
    public Response getUpdates(@PathParam("updaterId") int updaterId) {
        GraphUpdaterManager updaterManager = router.graph.updaterManager;
        if (updaterManager == null) {
            return Response.status(Response.Status.NOT_FOUND).entity("No updaters running.")
                    .build();
        }
        GraphUpdater updater = updaterManager.getUpdater(updaterId);
        if (updater == null) {
            return Response.status(Response.Status.NOT_FOUND).entity("No updater with that ID.")
                    .build();
        }

        // TODO provide default implementation in the interface (Java 8) and remove this
        Object result;
        try {
            Method m = updater.getClass().getMethod("getUpdates");
            result = m.invoke(updater);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            result = "Updates List not implemented in this updater";
        }

        return Response.status(Response.Status.OK).entity(result).build();
    }

}
