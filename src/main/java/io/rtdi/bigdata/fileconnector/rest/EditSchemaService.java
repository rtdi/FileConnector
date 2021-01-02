package io.rtdi.bigdata.fileconnector.rest;

import java.io.File;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBSuccessResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData;

@Path("/")
public class EditSchemaService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public EditSchemaService() {
	}
			
	@GET
	@Path("/fileschemas/{connectionname}/{schemaname}/template")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_SCHEMA})
    public Response getSchemaEditTemplate(@PathParam("connectionname") String connectionname, @PathParam("schemaname") String schemaname) {
		try {
			return Response.ok(new EditSchemaData()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/fileschemas/{connectionname}/{schemaname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getSchemaEdit(@PathParam("connectionname") String connectionname, @PathParam("schemaname") String schemaname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			File schemadir = EditSchemaData.getSchemaDirectory(connection);
			File schemafile = EditSchemaData.getSchemaFile(schemadir, schemaname);
			return Response.ok(new EditSchemaData(schemafile)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/fileschemas/{connectionname}/{schemaname}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response setSchema(@PathParam("connectionname") String connectionname, @PathParam("schemaname") String schemaname, EditSchemaData data) {
		try {
			if (schemaname == null) {
				return JAXBErrorResponseBuilder.getJAXBResponse("No schemaname provided");
			}
			if (schemaname.contains("..")) {
				return JAXBErrorResponseBuilder.getJAXBResponse("Schemaname contains invalid characters");
			}
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			File schemadir = EditSchemaData.getSchemaDirectory(connection);
			if (!schemadir.exists()) {
				schemadir.mkdirs();
			}
			File schemafile = EditSchemaData.getSchemaFile(schemadir, schemaname);
			data.writeSchema(schemafile);
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}