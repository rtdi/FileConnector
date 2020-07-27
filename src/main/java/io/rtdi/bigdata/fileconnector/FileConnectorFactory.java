package io.rtdi.bigdata.fileconnector;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class FileConnectorFactory extends ConnectorFactory<FileConnectionProperties, FileProducerProperties, FileConsumerProperties> {

	public FileConnectorFactory() {
		super("FileConnector");
	}

	@Override
	public Consumer<FileConnectionProperties, FileConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
		return new FileConsumer(instance);
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
	public FileConsumerProperties createConsumerProperties(String name) throws PropertiesException {
		return new FileConsumerProperties(name);
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
	public Service createService(ServiceController instance) throws PropertiesException {
		return null;
	}

	@Override
	public ServiceProperties createServiceProperties(String servicename) throws PropertiesException {
		return null;
	}

	@Override
	public boolean supportsConnections() {
		return true;
	}

	@Override
	public boolean supportsServices() {
		return false;
	}

	@Override
	public boolean supportsProducers() {
		return true;
	}

	@Override
	public boolean supportsConsumers() {
		return true;
	}

	@Override
	public boolean supportsBrowsing() {
		return true;
	}

}
