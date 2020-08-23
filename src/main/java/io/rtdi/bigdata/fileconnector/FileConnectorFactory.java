package io.rtdi.bigdata.fileconnector;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class FileConnectorFactory extends ConnectorFactory<FileConnectionProperties>
implements IConnectorFactoryProducer<FileConnectionProperties, FileProducerProperties> {

	public FileConnectorFactory() {
		super("FileConnector");
	}

	@Override
	public Producer<FileConnectionProperties, FileProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
		return new FileProducer(instance);
	}

	@Override
	public FileConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new FileConnectionProperties(name);
	}

	@Override
	public FileProducerProperties createProducerProperties(String name) throws PropertiesException {
		return new FileProducerProperties(name);
	}

	@Override
	public BrowsingService<FileConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new FileBrowser(controller);
	}

	@Override
	public boolean supportsBrowsing() {
		return true;
	}

}
