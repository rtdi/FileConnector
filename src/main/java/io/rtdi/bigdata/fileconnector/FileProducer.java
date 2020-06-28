package io.rtdi.bigdata.fileconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.avro.Schema;

import com.univocity.parsers.common.DataProcessingException;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.common.processor.ObjectRowProcessor;
import com.univocity.parsers.conversions.Conversions;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import io.rtdi.bigdata.connector.connectorframework.ProducerQueuing;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ThreadBasedController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData.ColumnDefinition;
import io.rtdi.bigdata.fileconnector.rest.FilePreviewService;

public class FileProducer extends ProducerQueuing<FileConnectionProperties, FileProducerProperties> {

	private long pollinterval;
	private FilenameFilter filter;
	private List<File> filelist;
	private File directory;
	private TopicHandler topichandler;
	private String producername;
	private EditSchemaData format;
	private SchemaHandler schemahandler;

	public FileProducer(ProducerInstanceController instance) throws IOException {
		super(instance);
		pollinterval = getProducerProperties().getPollInterval();
		directory = new File(getConnectionProperties().getRootDirectory());
		if (!directory.exists()) {
			throw new PropertiesException("The specified root directory does not exist", "Check the connection properties this producer belongs to", null, directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("The specified root directory exists but is not a directory", "Check the connection properties this producer belongs to", null, directory.getAbsolutePath());
		}
		String schemafilename = getProducerProperties().getSchemaFile();
		try {
			if (schemafilename == null) {
				throw new PropertiesException("No schema file name specified", "check the producer settings", null, schemafilename);
			}
			File schemadir = EditSchemaData.getSchemaDirectory(getConnectionController());
			File schemafile = EditSchemaData.getSchemaFile(schemadir, schemafilename);
			format = new EditSchemaData(schemafile);
			Pattern filepattern = Pattern.compile(format.getFilenamepattern());
			filter = new FileFilter(filepattern);
		} catch (PatternSyntaxException e) {
			throw new PropertiesException("The filename pattern is not valid", "Check the pattern if it is a true regexp syntax", null, schemafilename);
		}
		this.producername = getProducerProperties().getName();
	}

	@Override
	public void startProducerChangeLogging() throws IOException {
	}

	public ThreadBasedController<?> getNewQueueProducer() {
		return new FileParser(instance.getName());
	}
		
	protected Schema createSchema(String sourceschemaname) throws SchemaException, IOException {
		return format.createSchema();
	}
	
	@Override
	public void createTopiclist() throws IOException {
		topichandler = getTopic(getProducerProperties().getTargetTopic());
		if (topichandler == null) {
			topichandler = getPipelineAPI().getTopicOrCreate(getProducerProperties().getTargetTopic(), 1, (short) 1);
		}
		String fileschemaname = getProducerProperties().getSchemaFile();
		schemahandler = getSchemaHandler(fileschemaname);
		if (topichandler == null) {
			throw new PropertiesException("The specified target topic does not exist", "Check producer properties", null, getProducerProperties().getTargetTopic());
		} else if (schemahandler == null) {
			throw new PropertiesException("The specified target schema does not exist", "Check producer properties", null, getProducerProperties().getTargetTopic());
		} else {
			this.addTopicSchema(topichandler, schemahandler);
		}
	}

	@Override
	public String getLastSuccessfulSourceTransaction() throws IOException {
		// The file producer does not "restart". It simply reads all files not renamed yet.
		return null;
	}

	@Override
	public void initialLoad() throws IOException {
		// Reads all files just as the realtime push does, hence nothing to do special here.
	}

	@Override
	public void restartWith(String lastsourcetransactionid) throws IOException {
		// The file producer does not "restart". It simply reads all files not renamed yet.
	}

	@Override
	public long getPollingInterval() {
		return pollinterval;
	}

	@Override
	public void closeImpl() {
	}
	
	public class AvroRowProcessor extends ObjectRowProcessor {
		int rownumber = 1;
		private File file;
		private String transactionid;
		
	    public AvroRowProcessor() {
	    	for (int i=0; i<format.getColumns().size(); i++) {
	    		ColumnDefinition col = format.getColumns().get(i);
	    		IAvroDatatype dt = col.getAvroDatatype();
	    		String formats = col.getPatterns();
	    		String[] formatpatterns = null;
	    		if (formats != null) {
	    			formatpatterns = formats.replace('\r', ' ').split("\\n");
	    		}
	    		switch (dt.getAvroType()) {
	    		// Strings
				case AVROCLOB:
				case AVRONCLOB:
				case AVRONVARCHAR:
				case AVROSTRING:
				case AVROURI:
				case AVROUUID:
				case AVROVARCHAR:
					break;
					
				// Numbers
				case AVROBYTE:
					convertIndexes(Conversions.toByte()).set(i);
					break;
				case AVRODECIMAL:
					convertIndexes(Conversions.toBigDecimal()).set(i);
					break;
				case AVRODOUBLE:
					convertIndexes(Conversions.toDouble()).set(i);
					break;
				case AVROFLOAT:
					convertIndexes(Conversions.toFloat()).set(i);
					break;
				case AVROINT:
					convertIndexes(Conversions.toInteger()).set(i);
					break;
				case AVROLONG:
					convertIndexes(Conversions.toLong()).set(i);
					break;
				case AVROSHORT:
					convertIndexes(Conversions.toShort()).set(i);
					break;

				// Date/Time
				case AVRODATE:
					convertIndexes(Conversions.toDate(formatpatterns)).set(i);
					break;
				case AVROTIMEMICROS:
					// convertIndexes(Conversions.to????()).set(i);
					break;
				case AVROTIMEMILLIS:
					// convertIndexes(Conversions.to????()).set(i);
					break;
				case AVROTIMESTAMPMICROS:
					convertIndexes(Conversions.toDate(formatpatterns)).set(i);
					break;
				case AVROTIMESTAMPMILLIS:
					convertIndexes(Conversions.toDate(formatpatterns)).set(i);
					break;

				case AVROBOOLEAN:
					convertIndexes(Conversions.toBoolean()).set(i);
					break;

				// Not supported yet
				case AVROANYPRIMITIVE:
					break;
				case AVROARRAY:
					break;
				case AVROBYTES:
					break;
				case AVROENUM:
					break;
				case AVROFIXED:
					break;
				case AVROMAP:
					break;
				case AVRORECORD:
					break;
				case AVROSTGEOMETRY:
					break;
				case AVROSTPOINT:
					break;
				default:
					break;
	    		
	    		}
	    	}
		}

		@Override
	    public void rowProcessed(Object[] row, ParsingContext context) {
			logger.debug("parsing record {}", row);
			String rownum = String.valueOf(rownumber);
			JexlRecord valuerecord = new JexlRecord(schemahandler.getValueSchema());
			valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, producername);
			valuerecord.put("FILENAME", file.getName());
			valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, rownum);
			
			for (int i=0; i<context.headers().length; i++) {
				String header = context.headers()[i];
				valuerecord.put(header, row[i]);
			}
			

			logger.debug("Queueing record {}", valuerecord.toString());
			queueRecord(topichandler, null, schemahandler, valuerecord, RowType.INSERT, 
					rownum, producername, transactionid);
			rownumber++;
	    }
	    
	    public void setFile(File file) {
	    	this.file = file;
	    }

		public void setTransactionID(String transactionid) {
			this.transactionid = transactionid;
		}

	}

	private class FileParser extends ThreadBasedController<FileParser> {
		
		public FileParser(String name) {
			super(name);
		}

		@Override
		protected void runUntilError() throws Exception {
			CsvParserSettings settings = format.getSettings();
			AvroRowProcessor rowProcessor = new AvroRowProcessor();
			settings.setProcessor(rowProcessor);
			while (isRunning()) {
				filelist = readDirectory();
				if (filelist != null) {
					for (File file : filelist) {
						logger.info("Reading File {}", file.getName());
						rowProcessor.setFile(file);
						String transactionid = file.getName();
						rowProcessor.setTransactionID(transactionid);
						try {
							try (FileInputStream in = new FileInputStream(file); ) {
								queueBeginTransaction(transactionid, file);
								CsvParser parser = new CsvParser(settings);
								parser.parse(in, FilePreviewService.getCharset(format));
								queueCommitRecord();
							} catch (IOException e) {
								throw new ConnectorRuntimeException(
										"IOException when parsing the file",
										e,
										"Check the format definitions with the file contents",
										file.getAbsolutePath() + " # at line " + rowProcessor.rownumber);
							} catch (DataProcessingException e) {
								throw new ConnectorRuntimeException(
										"Structural parsing of a row was correct but the row content triggered an exception",
										e,
										"Very likely the column value of this row did not match the format definition",
										file.getAbsolutePath() + " # at line " + rowProcessor.rownumber);
							} catch (TextParsingException e) {
								throw new ConnectorRuntimeException(
										"Structural parsing of a row failed",
										e,
										"Please check the corresponding data line",
										file.getAbsolutePath() + " # at line " + rowProcessor.rownumber);
							}
						} catch (ConnectorRuntimeException e) {
							errors.addError(e);
							logger.error(e);
							queueRollbackRecord();
							renameToError(file);
						}
					}
				}
				waitTransactionsCompleted();
				sleep(getPollingInterval()*1000L);
			}
		}

		@Override
		protected String getControllerType() {
			return "FileParser";
		}

		@Override
		protected void startThreadControllerImpl() throws IOException {
		}

		@Override
		protected void stopThreadControllerImpl(ControllerExitType exittype) {
			
		}

		@Override
		protected void updateLandscape() {
		}

		@Override
		protected void updateSchemaCache() {
		}
	}
	
	@Override
	public void commit(String transactionid, Object payload) throws ConnectorRuntimeException {
		File file = (File) payload;
		if (file != null) {
			File newfile = new File(file.getAbsolutePath() + ".processed");
			try {
				Files.move(file.toPath(), newfile.toPath(), StandardCopyOption.ATOMIC_MOVE);
			} catch (IOException e) {
				throw new ConnectorRuntimeException("Cannot rename the file to .processed", e, null, transactionid);
			}
		}
	}

	public void renameToError(File file) throws ConnectorRuntimeException {
		if (file != null) {
			File newfile = new File(file.getAbsolutePath() + ".error");
			try {
				Files.move(file.toPath(), newfile.toPath(), StandardCopyOption.ATOMIC_MOVE);
			} catch (IOException e) {
				throw new ConnectorRuntimeException("Cannot rename the file to .error", e, null, null);
			}
		}
	}


	private List<File> readDirectory() {
		File[] files = directory.listFiles(filter);
		if (files != null) {
			return Arrays.asList(files);
		} else {
			return null;
		}
	}
	
	private class FileFilter implements FilenameFilter {
		private Pattern filepattern;

		public FileFilter(Pattern filepattern) {
			this.filepattern = filepattern;
		}

		@Override
		public boolean accept(File dir, String name) {
			if (name.endsWith(".processed")) { // failsafe in case the pattern is something like .* 
				return false;
			}
			Matcher matcher = filepattern.matcher(name);
	        boolean matches = matcher.matches();
	        if (getProducerController().getInstanceCount() > 1) {
	        	/*
	        	 * In case of multiple producer instances, each producer reads one file only. Which one is derived from the file name hash value.
	        	 */
	        	int hash = name.hashCode();
	        	matches &= (hash % getProducerController().getInstanceCount()) == getProducerInstance().getInstanceNumber();
	        }
	        return matches;
		}
		
	}

}
