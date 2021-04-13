package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by lazyman on 29/01/2017.
 */
public class ObjectClassHandlerConfiguration {

    private static final Log LOG = Log.getLog(ObjectClassHandlerConfiguration.class);

    private ObjectClass objectClass;
    //TODO DIEGO: S3
    //private File filePath;
    private String bucketName;
    private String fileName;
    private String region;

    private String encoding;

    private String fieldDelimiter;
    private String escape;
    private String commentMarker;
    private boolean ignoreEmptyLines = true;
    private String quote;
    private String quoteMode;
    private String recordSeparator;
    private boolean ignoreSurroundingSpaces = false;
    private boolean trailingDelimiter = false;
    private boolean trim = false;
    private boolean headerExists = true;

    private String uniqueAttribute;
    private String nameAttribute;
    private String passwordAttribute;

    private String multivalueDelimiter;

    private String multivalueAttributes;

    private int preserveOldSyncFiles = 10;
    private File tmpFolder;

    private boolean readOnly = false;

    private boolean ignoreIdentifierCase = false;

    private boolean container = false;
    private boolean auxiliary = false;

    private CloudStorageService cloudStorageService;

    public ObjectClassHandlerConfiguration() {
        this(ObjectClass.ACCOUNT, null);
    }

    public ObjectClassHandlerConfiguration(ObjectClass oc, Map<String, Object> values) {
        this.objectClass = oc;

        //setFilePath(Util.getSafeValue(values, "filePath", null, File.class));
        setRegion(Util.getSafeValue(values, "region", "us-east-1"));
        this.cloudStorageService = new CloudStorageService(this.getRegion());
        setBucketName(Util.getSafeValue(values, "bucketName", null));
        setFileName(Util.getSafeValue(values, "fileName", null));
        setEncoding(Util.getSafeValue(values, "encoding", "utf-8"));
        setTmpFolder(Util.getSafeValue(values, "tmpFolder", null, File.class));
        setFieldDelimiter(Util.getSafeValue(values, "fieldDelimiter", ";"));
        setEscape(Util.getSafeValue(values, "escape", "\\"));
        setCommentMarker(Util.getSafeValue(values, "commentMarker", "#"));
        setIgnoreEmptyLines(Util.getSafeValue(values, "ignoreEmptyLines", true, Boolean.class));
        setQuote(Util.getSafeValue(values, "quote", "\""));
        setQuoteMode(Util.getSafeValue(values, "quoteMode", QuoteMode.MINIMAL.name()));
        setRecordSeparator(Util.getSafeValue(values, "recordSeparator", "\r\n"));
        setIgnoreSurroundingSpaces(Util.getSafeValue(values, "ignoreSurroundingSpaces", false, Boolean.class));
        setTrailingDelimiter(Util.getSafeValue(values, "trailingDelimiter", false, Boolean.class));
        setTrim(Util.getSafeValue(values, "trim", false, Boolean.class));
        setHeaderExists(Util.getSafeValue(values, "headerExists", true, Boolean.class));

        setUniqueAttribute(Util.getSafeValue(values, "uniqueAttribute", null));
        setNameAttribute(Util.getSafeValue(values, "nameAttribute", null));
        setPasswordAttribute(Util.getSafeValue(values, "passwordAttribute", null));
        setMultivalueAttributes(Util.getSafeValue(values, "multivalueAttributes", null));

        setMultivalueDelimiter(Util.getSafeValue(values, "multivalueDelimiter", null));

        setPreserveOldSyncFiles(Util.getSafeValue(values, "preserveOldSyncFiles", 10, Integer.class));

        setReadOnly(Util.getSafeValue(values, "readOnly", false, Boolean.class));

        setIgnoreIdentifierCase(Util.getSafeValue(values, "ignoreIdentifierCase", false, Boolean.class));

        setContainer(Util.getSafeValue(values, "container", false, Boolean.class));
        setAuxiliary(Util.getSafeValue(values, "auxiliary", false, Boolean.class));
    }

    public void recompute() {
        /*if (tmpFolder == null && filePath != null) {
            this.tmpFolder = filePath.getParentFile();
        }*/

        if (StringUtil.isEmpty(nameAttribute)) {
            nameAttribute = uniqueAttribute;
        }
    }

    public boolean isContainer() {
        return container;
    }

    public void setContainer(boolean container) {
        this.container = container;
    }

    public boolean isAuxiliary() {
        return auxiliary;
    }

    public void setAuxiliary(boolean auxiliary) {
        this.auxiliary = auxiliary;
    }

    public String getMultivalueAttributes() {
        return multivalueAttributes;
    }

    public void setMultivalueAttributes(String multivalueAttributes) {
        this.multivalueAttributes = multivalueAttributes;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public File getTmpFolder() {
        return tmpFolder;
    }
    public void setTmpFolder(File tmpFolder) {
        this.tmpFolder = tmpFolder;
    }

    public boolean isHeaderExists() {
        return headerExists;
    }

    public void setHeaderExists(boolean headerExists) {
        this.headerExists = headerExists;
    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }

    public void setObjectClass(ObjectClass objectClass) {
        this.objectClass = objectClass;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public CloudStorageService getCloudStorageService() {
        return cloudStorageService;
    }

    public void setCloudStorageService(CloudStorageService cloudStorageService) {
        this.cloudStorageService = cloudStorageService;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getEscape() {
        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public String getCommentMarker() {
        return commentMarker;
    }

    public void setCommentMarker(String commentMarker) {
        this.commentMarker = commentMarker;
    }

    public boolean isIgnoreEmptyLines() {
        return ignoreEmptyLines;
    }

    public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public String getQuoteMode() {
        return quoteMode;
    }

    public void setQuoteMode(String quoteMode) {
        this.quoteMode = quoteMode;
    }

    public String getRecordSeparator() {
        return recordSeparator;
    }

    public void setRecordSeparator(String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    public boolean isIgnoreSurroundingSpaces() {
        return ignoreSurroundingSpaces;
    }

    public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    public boolean isTrailingDelimiter() {
        return trailingDelimiter;
    }

    public void setTrailingDelimiter(boolean trailingDelimiter) {
        this.trailingDelimiter = trailingDelimiter;
    }

    public boolean isTrim() {
        return trim;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    public String getUniqueAttribute() {
        return uniqueAttribute;
    }

    public void setUniqueAttribute(String uniqueAttribute) {
        this.uniqueAttribute = uniqueAttribute;

        if (this.nameAttribute == null) {
            this.nameAttribute = uniqueAttribute;
        }
    }

    public String getNameAttribute() {
        return nameAttribute;
    }

    public void setNameAttribute(String nameAttribute) {
        this.nameAttribute = nameAttribute;
    }

    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    public void setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    public String getMultivalueDelimiter() {
        return multivalueDelimiter;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        this.multivalueDelimiter = multivalueDelimiter;
    }

    public int getPreserveOldSyncFiles() {
        return preserveOldSyncFiles;
    }

    public void setPreserveOldSyncFiles(int preserveOldSyncFiles) {
        this.preserveOldSyncFiles = preserveOldSyncFiles;
    }

    public boolean isIgnoreIdentifierCase() {
        return ignoreIdentifierCase;
    }

    public void setIgnoreIdentifierCase(boolean ignoreIdentifierCase) {
        this.ignoreIdentifierCase = ignoreIdentifierCase;
    }

    public void validate() {
        LOG.ok("Validating configuration for {0}", objectClass);

        validateCsvFile();
        validateTmpFolder();

        Util.notEmpty(encoding, "Encoding is not defined.");

        if (!Charset.isSupported(encoding)) {
            throw new ConfigurationException("Encoding '" + encoding + "' is not supported");
        }

        Util.notEmpty(fieldDelimiter, "Field delimiter can't be null or empty");
        Util.notEmpty(escape, "Escape character is not defined");
        Util.notEmpty(commentMarker, "Comment marker character is not defined");
        Util.notEmpty(quote, "Quote character is not defined");

        Util.notEmpty(quoteMode, "Quote mode is not defined");
        boolean found = false;
        for (QuoteMode qm : QuoteMode.values()) {
            if (qm.name().equalsIgnoreCase(quoteMode)) {
                found = true;
                break;
            }
        }
        if (!found) {
            StringBuilder sb = new StringBuilder();
            for (QuoteMode qm : QuoteMode.values()) {
                sb.append(qm.name()).append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            throw new ConfigurationException("Quote mode '" + quoteMode + "' is not supported, supported values: ["
                    + sb.toString() + "]");
        }

        Util.notEmpty(recordSeparator, "Record separator is not defined");

        validateAttributeNames();
    }

    private void validateCsvFile() {
        cloudStorageService.checkCanReadFileS3(bucketName,fileName);

    	/*synchronized (CsvCloudObjectStorageConnector.SYNCH_FILE_LOCK) {
    		if (!readOnly && !filePath.canWrite()) {
    			throw new ConfigurationException("Can't write to file '" + filePath.getAbsolutePath() + "'");
    		}
    	}*/
    }

    private void validateAttributeNames() {
        if (StringUtil.isEmpty(uniqueAttribute)) {
            throw new ConfigurationException("Unique attribute is not defined.");
        }

        if (StringUtil.isEmpty(nameAttribute)) {
            LOG.ok("Name attribute not defined, value from unique attribute will be used (" + uniqueAttribute + ").");
            nameAttribute = uniqueAttribute;
        }

        if (StringUtil.isEmpty(passwordAttribute)) {
            LOG.ok("Password attribute is not defined.");
        }
    }

    private void validateTmpFolder() {
        if (tmpFolder == null) {
            throw new ConfigurationException("Tmp folder path is not defined");
        }
        if (!tmpFolder.exists()) {
            throw new ConfigurationException("Tmp folder '" + tmpFolder + "' doesn't exists");
        }
        if (!tmpFolder.isDirectory()) {
            throw new ConfigurationException("Tmp folder '" + tmpFolder + "' is not a directory");
        }
        if (!tmpFolder.canRead()) {
            throw new ConfigurationException("Can't read from tmp folder '" + tmpFolder + "'");
        }
        if (!tmpFolder.canWrite()) {
            throw new ConfigurationException("Can't write to tmp folder '" + tmpFolder + "'");
        }
    }

}
