package io.rtdi.bigdata.fileconnector.entity;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.commons.text.StringEscapeUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.univocity.parsers.csv.CsvParserSettings;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.KeyValue;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroBoolean;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroByte;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroCLOB;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDate;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDecimal;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDouble;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroFloat;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroLong;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNCLOB;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroShort;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestampMicros;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class EditSchemaData {
	private static final String FILENAMEPATTERN = "FILENAMEPATTERN";
	private static final String LOCALE = "LOCALE";
	private static final String CHARSET = "CHARSET";
	private static final String DELIMITER = "DELIMITER";
	private static final String LINESEPARATOR = "LINESEPARATOR";
	private static final String QUOTE = "QUOTE";
	private static final String QUOTEESCAPE = "QUOTEESCAPE";
	private static final String ISHEADEREXTRACTIONENABLED = "ISHEADEREXTRACTIONENABLED";
	private static final String FORMATPATTERNS = "FORMATPATTERNS";
	
	private String schemaname;
	private String description;
	private String filenamepattern;
	private String charset;
	private String locale;
	private CsvParserSettings settings = new CsvParserSettings();
	
	private List<ColumnDefinition> columns;
	private List<KeyValue> datatypes;
	
	private static String defaultcharset = Charset.defaultCharset().name();
	private static String defaultlocale = Locale.getDefault().toString();
		
	public EditSchemaData() {
		super();
		locale = defaultlocale;
		charset = defaultcharset;
		setDelimiter(";");
		setHeaderExtractionEnabled(true);
		setLineSeparator("\n");
		setQuote("\"");
		setQuoteEscape("\"");
		datatypes = new ArrayList<KeyValue>();
		datatypes.add(new KeyValue(AvroCLOB.NAME));
		datatypes.add(new KeyValue(AvroNCLOB.NAME));
		datatypes.add(new KeyValue(AvroNVarchar.NAME + "( )"));
		datatypes.add(new KeyValue(AvroVarchar.NAME + "( )"));
		datatypes.add(new KeyValue(AvroByte.NAME));
		datatypes.add(new KeyValue(AvroDecimal.NAME+ "( , )"));
		datatypes.add(new KeyValue(AvroDouble.NAME));
		datatypes.add(new KeyValue(AvroFloat.NAME));
		datatypes.add(new KeyValue(AvroInt.NAME));
		datatypes.add(new KeyValue(AvroLong.NAME));
		datatypes.add(new KeyValue(AvroShort.NAME));
		datatypes.add(new KeyValue(AvroDate.NAME));
		datatypes.add(new KeyValue(AvroTimestamp.NAME));
		datatypes.add(new KeyValue(AvroTimestampMicros.NAME));
		datatypes.add(new KeyValue(AvroBoolean.NAME));
	}

	public EditSchemaData(File schemafile) throws IOException {
		this();
		if (!schemafile.exists()) {
			throw new ConnectorCallerException(
					"The file with the schema definition cannot be found", null,
					"In the webapp root directory, the schema subdirectory, all *.avsc file are stored", 
					schemafile.getPath());
		}
		if (!schemafile.canRead()) {
			throw new ConnectorCallerException("The file with the schema definition exists but cannot be read by the OS user", null,
					"Check user running tomcat and its permissions", schemafile.getPath());
		}
		Schema schema = new Parser().parse(schemafile);
		setSchemaname(schema.getFullName());
		setDescription(schema.getDoc());
		setCharset(schema.getProp(CHARSET));
		setLocale(schema.getProp(LOCALE));
		setFilenamepattern(schema.getProp(FILENAMEPATTERN));
		setDelimiter(schema.getProp(DELIMITER));
		setLineSeparator(schema.getProp(LINESEPARATOR));
		setQuote(schema.getProp(QUOTE));
		setQuoteEscape(schema.getProp(QUOTEESCAPE));
		setHeaderExtractionEnabled(schema.getProp(ISHEADEREXTRACTIONENABLED).equalsIgnoreCase("true"));
		List<ColumnDefinition> columns = new ArrayList<>();
		for (Field f : schema.getFields()) {
			String name = AvroField.getOriginalName(f);
			if (!AvroField.isInternal(f)) {
				ColumnDefinition c = new ColumnDefinition(name, AvroType.getAvroDataType(IOUtils.getBaseSchema(f.schema())), f.schema().getType() == Type.UNION);
				c.setPatterns(f.getProp(FORMATPATTERNS));
				columns.add(c);
			}
		}
		setColumns(columns);
	}

	public String getSchemaname() {
		return schemaname;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}

	public String getFilenamepattern() {
		return filenamepattern;
	}

	public void setFilenamepattern(String filenamepattern) {
		this.filenamepattern = filenamepattern;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) throws ConnectorCallerException {
		if (charset != null) {
			if (Charset.availableCharsets().containsKey(charset)) {
				this.charset = charset;
			} else {
				throw new ConnectorCallerException("The provided charset is not known to this system", null, "Maybe change to a different character set?", charset);
			}
		} else {
			this.charset = null;
		}
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) throws ConnectorCallerException {
		if (locale != null) {
			boolean found = false;
			for (Locale e : Locale.getAvailableLocales()) {
				if (e.toString().equals(locale)) {
					found = true;
					break;
				}
			}
			if (found) {
				this.locale = locale;
			} else {
				throw new ConnectorCallerException("The provided locale is not known to this system", null, "Maybe change to a different locale?", locale);
			}
		} else {
			this.locale = null;
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getLineSeparator() {
		return StringEscapeUtils.escapeJava(settings.getFormat().getLineSeparatorString());
	}

	public void setLineSeparator(String lineseparator) {
		settings.getFormat().setLineSeparator(StringEscapeUtils.unescapeJava(lineseparator));
	}

	public String getDelimiter() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getDelimiter()));
	}

	public void setDelimiter(String delimiter) {
		settings.getFormat().setDelimiter(StringEscapeUtils.unescapeJava(delimiter));
	}
	
	public String getQuoteEscape() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getQuoteEscape()));
	}

	public void setQuoteEscape(String quoteescape) {
		String s = StringEscapeUtils.unescapeJava(quoteescape);
		if (s != null && s.length() > 0) {
			settings.getFormat().setQuoteEscape(s.charAt(0));
		}
	}

	public String getQuote() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getQuote()));
	}

	public void setQuote(String quote) {
		String s = StringEscapeUtils.unescapeJava(quote);
		if (s != null && s.length() > 0) {
			settings.getFormat().setQuote(s.charAt(0));
		}
	}

	public String getCharToEscapeQuoteEscaping() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getCharToEscapeQuoteEscaping()));
	}

	public void setCharToEscapeQuoteEscaping(String escape) {
		String s = StringEscapeUtils.unescapeJava(escape);
		if (s != null && s.length() > 0) {
			settings.getFormat().setCharToEscapeQuoteEscaping(s.charAt(0));
		}
	}

	public boolean isHeaderExtractionEnabled() {
		return settings.isHeaderExtractionEnabled();
	}

	public void setHeaderExtractionEnabled(boolean headerextractionenabled) {
		settings.setHeaderExtractionEnabled(headerextractionenabled);
	}
	
	@XmlTransient
	public CsvParserSettings getSettings() {
		return settings;
	}
	
	public List<ColumnDefinition> getColumns() {
		return columns;
	}
	
	public void setColumns(List<ColumnDefinition> columns) {
		this.columns = columns;
		if (columns != null) {
			String[] headers = new String[columns.size()];
			for( int i=0; i<columns.size(); i++) {
				headers[i] = columns.get(i).getName();
			}
			settings.setHeaders(headers);
		} else {
			settings.setHeaders((String[]) null); 
		}
	}
	
	public void updateHeader(String[] headers, List<String[]> rows, boolean reset) {
		if (columns == null || reset) {
			columns = new ArrayList<>();
			for (int i=0; i<headers.length; i++) {
				String name = headers[i];
				columns.add(new ColumnDefinition(name, rows, i, true));
			}
			settings.setHeaders(headers);
		} else {
			for (int i=0; i<headers.length; i++) {
				if (i<columns.size()) {
					// do not change the column definitions but adopt the header name to the entered one
					headers[i] = columns.get(i).getName();
				} else {
					columns.add(new ColumnDefinition(headers[i], rows, i, true));
				}
			}
			while (columns.size() > headers.length) {
				columns.remove(columns.size()-1);
			}
			settings.setHeaders(headers);
		}
	}
	
	public void writeSchema(File schemafile) throws SchemaException, IOException {
		String avsc = createSchema().toString(true);
		try (FileWriter out = new FileWriter(schemafile);) {
			out.write(avsc);
		}
	}

	public Schema createSchema() throws SchemaException, IOException {
		ValueSchema schema = new ValueSchema(schemaname, description);
		Schema s = schema.getSchema();
		s.addProp(FILENAMEPATTERN, filenamepattern);
		s.addProp(LOCALE, locale);
		s.addProp(CHARSET, charset);
		s.addProp(DELIMITER, getDelimiter());
		s.addProp(LINESEPARATOR, getLineSeparator());
		s.addProp(QUOTE, getQuote());
		s.addProp(QUOTEESCAPE, getQuoteEscape());
		s.addProp(ISHEADEREXTRACTIONENABLED, String.valueOf(isHeaderExtractionEnabled()));
		
		schema.add("FILENAME", 
				AvroVarchar.getSchema(300),
				"Name of the file", 
				true).setPrimaryKey().setInternal();

		if (columns == null) {
			schema.add("col1", AvroNVarchar.getSchema(1000), null, true); // Avro Schemas need at least one field
		} else {
			for (ColumnDefinition column : columns) {
				
				Schema dt = column.datatype.getDatatypeSchema();
				if (dt == null) {
					dt = AvroNVarchar.getSchema(1000);
				}
				if (column.getPatterns() != null) {
					dt.addProp(FORMATPATTERNS, column.getPatterns());
				}
				schema.add(column.name, dt, null, column.optional);
			}
		}
		schema.build();
		return schema.getSchema();
	}

	public static File getSchemaFile(File schemadir, String schemaname) {
		return new File(schemadir, schemaname + ".avsc");
	}
	
	public static File getSchemaDirectory(File rootdirectory) {
		return new File(rootdirectory, "schemas");
	}

	public static File getSchemaDirectory(ConnectionController connection) {
		return getSchemaDirectory(connection.getDirectory());
	}

	public List<KeyValue> getDatatypes() {
		return datatypes;
	}

	public void setDatatypes(List<KeyValue> datatypes) {
		// do nothing
	}

	public static class ColumnDefinition {

		private String name;
		private IAvroDatatype datatype;
		private boolean optional = true;
		private String patterns;

		private AvroType typeaggregation = null;
		private int lengthaggregation = 0;
		private int scaleaggregation = 0;
        private static Pattern decimal = Pattern.compile("[-]?\\d*[\\.\\,]\\d*");
        private static Pattern integer = Pattern.compile("[-]?\\d*");
        private static Pattern date_yyyymmdd = Pattern.compile("\\d{4}\\-\\d{2}\\-\\d{2}");
        private static Pattern date_yyyymmdd2 = Pattern.compile("\\d{4}\\.\\d{2}\\.\\d{2}");
        private static Pattern timestamp_rfc = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T]\\d{2}:\\d{2}:\\d{2}(Z|[+-]\\d{4})");
        private static Pattern timestamp_simple = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[ ]\\d{2}:\\d{2}:\\d{2}");
        private int integermaxlength = String.valueOf(Integer.MAX_VALUE).length();

		public ColumnDefinition() {
		}
		
		public ColumnDefinition(String header, IAvroDatatype datatype, boolean optional) {
			this.name = header;
			this.datatype = datatype;
			this.optional = optional;
		}

		public ColumnDefinition(String header, List<String[]> rows, int columnindex, boolean optional) {
			this.name = header;
			this.optional = optional;
			for (int r = 0; r<rows.size(); r++) {
				String value = rows.get(r)[columnindex];
				mergeValue(value);
			}
			this.datatype = AvroType.getDataType(typeaggregation, lengthaggregation, scaleaggregation);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDatatype() {
			return datatype.toString();
		}

		public void setDatatype(String datatypestring) {
			this.datatype = AvroType.getDataTypeFromString(datatypestring);
		}

		public boolean isOptional() {
			return optional;
		}

		public void setOptional(boolean optional) {
			this.optional = optional;
		}
		
		@JsonIgnore
		public IAvroDatatype getAvroDatatype() {
			return datatype;
		}
		
		public String getPatterns() {
			return patterns;
		}

		public void setPatterns(String patterns) {
			this.patterns = patterns;
		}
		
		public void mergeValue(String value) {
			if (value != null) {
				if (lengthaggregation < value.length()) {
					lengthaggregation = value.length();
				}
		        if (decimal.matcher(value).matches()) {
		        	mergeType(AvroType.AVRODECIMAL);
		        	int s = value.substring(value.indexOf('.')).length();
		        	if (scaleaggregation < s) {
		        		scaleaggregation = s;
		        	}
		        } else if (integer.matcher(value).matches()) {
		        	if (lengthaggregation <= integermaxlength) {
		        		mergeType(AvroType.AVROINT);
		        	} else {
		        		mergeType(AvroType.AVROLONG);
		        	}
		        } else if (date_yyyymmdd.matcher(value).matches()) {
		        	mergeType(AvroType.AVRODATE);
		        	addPattern("yyyy-MM-dd");
		        } else if (date_yyyymmdd2.matcher(value).matches()) {
		        	mergeType(AvroType.AVRODATE);
		        	addPattern("yyyy.MM.dd");
		        } else if (timestamp_rfc.matcher(value).matches()) {
		        	mergeType(AvroType.AVROTIMESTAMPMILLIS);
		        	addPattern("yyyy-MM-ddTHH:mm:ssZ");
		        } else if (timestamp_simple.matcher(value).matches()) {
		        	mergeType(AvroType.AVROTIMESTAMPMILLIS);
		        	addPattern("yyyy-MM-dd HH:mm:ss");
		        } else if (lengthaggregation >= 5000) {
		        	mergeType(AvroType.AVRONCLOB);
		        } else {
		        	mergeType(AvroType.AVRONVARCHAR);
		        }
			}
		}
		
		private void addPattern(String text) {
			if (patterns == null) {
				patterns = text;
			} else {
				patterns += "\r\n" + text;
			}
		}
		
		private void mergeType(AvroType valuetype) {
        	if (typeaggregation == null) {
        		typeaggregation = valuetype;
        	} else if (typeaggregation == valuetype) {
        		// do nothing
        	} else {
        		typeaggregation = typeaggregation.aggregate(valuetype);
        	}
		}

	}
	
}
