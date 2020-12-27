package io.rtdi.bigdata.fileconnector.rest;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.fileconnector.entity.LocaleList;

@Path("/")
public class LocaleListService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public LocaleListService() {
	}
			
	@GET
	@Path("/locales")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFileContent() {
		try {
			return Response.ok(new LocaleList()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
	