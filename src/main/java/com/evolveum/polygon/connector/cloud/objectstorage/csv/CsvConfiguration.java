package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(CsvConfiguration.class);

    private ObjectClassHandlerConfiguration config = new ObjectClassHandlerConfiguration();

    private File objectClassDefinition = null;

    @ConfigurationProperty(
            displayMessageKey = "UI_PRESERVE_OLD_SYNC_FILES",
            helpMessageKey = "UI_PRESERVE_OLD_SYNC_FILES_HELP")
    public int getPreserveOldSyncFiles() {
        return config.getPreserveOldSyncFiles();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE",
            helpMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE_HELP", required = true)
    public String getUniqueAttribute() {
        return config.getUniqueAttribute();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_NAME_ATTRIBUTE",
            helpMessageKey = "UI_CSV_NAME_ATTRIBUTE_HELP")
    public String getNameAttribute() {
        return config.getNameAttribute();
    }


    /*
    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_FILE_PATH",
            helpMessageKey = "UI_CSV_FILE_PATH_HELP", required = true)
    public File getFilePath() {
        return config.getFilePath();
    }
    */

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_FILE_NAME",
            helpMessageKey = "UI_CSV_FILE_NAME_HELP", required = true)
    public String getFileName() {
        return config.getFileName();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_BUCKET_NAME",
            helpMessageKey = "UI_CSV_BUCKET_NAME_HELP", required = true)
    public String getBucketName() {
        return config.getBucketName();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_REGION_NAME",
            helpMessageKey = "UI_CSV_REGION_NAME_HELP", required = true)
    public String getRegion() {
        return config.getRegion();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE",
            helpMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE_HELP")
    public String getPasswordAttribute() {
        return config.getPasswordAttribute();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_ENCODING",
            helpMessageKey = "UI_CSV_ENCODING_HELP")
    public String getEncoding() {
        return config.getEncoding();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_FIELD_DELIMITER",
            helpMessageKey = "UI_CSV_FIELD_DELIMITER_HELP")
    public String getFieldDelimiter() {
        return config.getFieldDelimiter();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_ESCAPE",
            helpMessageKey = "UI_CSV_ESCAPE_HELP")
    public String getEscape() {
        return config.getEscape();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_COMMENT_MARKER",
            helpMessageKey = "UI_CSV_COMMENT_MARKER_HELP")
    public String getCommentMarker() {
        return config.getCommentMarker();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_IGNORE_EMPTY_LINES",
            helpMessageKey = "UI_CSV_IGNORE_EMPTY_LINES_HELP")
    public boolean isIgnoreEmptyLines() {
        return config.isIgnoreEmptyLines();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_QUOTE",
            helpMessageKey = "UI_CSV_QUOTE_HELP")
    public String getQuote() {
        return config.getQuote();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_QUOTE_MODE",
            helpMessageKey = "UI_CSV_QUOTE_MODE_HELP")
    public String getQuoteMode() {
        return config.getQuoteMode();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_RECORD_SEPARATOR",
            helpMessageKey = "UI_CSV_RECORD_SEPARATOR_HELP")
    public String getRecordSeparator() {
        return config.getRecordSeparator();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_IGNORE_SURROUNDING_SPACES",
            helpMessageKey = "UI_CSV_IGNORE_SURROUDING_SPACES_HELP")
    public boolean isIgnoreSurroundingSpaces() {
        return config.isIgnoreSurroundingSpaces();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_TRAILING_DELIMITER",
            helpMessageKey = "UI_CSV_TRAILING_DELIMITER_HELP")
    public boolean isTrailingDelimiter() {
        return config.isTrailingDelimiter();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_TRIM",
            helpMessageKey = "UI_CSV_TRIM_HELP")
    public boolean isTrim() {
        return config.isTrim();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER",
            helpMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER_HELP")
    public String getMultivalueDelimiter() {
        return config.getMultivalueDelimiter();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_OBJECT_CLASS_DEFINITION",
            helpMessageKey = "UI_CSV_OBJECT_CLASS_DEFINITION_HELP")
    public File getObjectClassDefinition() {
        return objectClassDefinition;
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_HEADER_EXISTS",
            helpMessageKey = "UI_CSV_HEADER_EXISTS_HELP")
    public boolean isHeaderExists() {
        return config.isHeaderExists();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_READ_ONLY",
            helpMessageKey = "UI_CSV_READ_ONLY_HELP")
    public boolean isReadOnly() {
        return config.isReadOnly();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CSV_TMP_FOLDER",
            helpMessageKey = "UI_CSV_TMP_FOLDER_HELP")
    public File getTmpFolder() {
        return config.getTmpFolder();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_IGNORE_IDENTIFIER_CASE",
            helpMessageKey = "UI_IGNORE_IDENTIFIER_CASE_HELP")
    public boolean isIgnoreIdentifierCase() {
        return config.isIgnoreIdentifierCase();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_MULTIVALUE_ATTRIBUTES",
            helpMessageKey = "UI_MULTIVALUE_ATTRIBUTES_HELP")
    public String getMultivalueAttributes() {
        return config.getMultivalueAttributes();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_CONTAINER",
            helpMessageKey = "UI_CONTAINER_HELP")
    public boolean isContainer() {
        return config.isContainer();
    }

    @ConfigurationProperty(
            displayMessageKey = "UI_AUXILIARY",
            helpMessageKey = "UI_AUXILIARY_HELP")
    public boolean isAuxiliary() {
        return config.isAuxiliary();
    }

    public void setContainer(boolean container) {
        config.setContainer(container);
    }

    public void setAuxiliary(boolean auxiliary) {
        config.setAuxiliary(auxiliary);
    }

    public void setMultivalueAttributes(String multivalueAttributes) {
        config.setMultivalueAttributes(multivalueAttributes);
    }

    public void setReadOnly(boolean readOnly) {
        config.setReadOnly(readOnly);
    }

    public void setTmpFolder(File tmpFolder) {
        config.setTmpFolder(tmpFolder);
    }

    public void setHeaderExists(boolean headerExists) {
        config.setHeaderExists(headerExists);
    }

    public void setObjectClassDefinition(File objectClassDefinition) {
        this.objectClassDefinition = objectClassDefinition;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        config.setMultivalueDelimiter(multivalueDelimiter);
    }

    /*public void setFilePath(File filePath) {
        config.setFilePath(filePath);
    }*/

    public void setFileName(String fileName) {
        config.setFileName(fileName);
    }

    public void setRegion(String region) {
        config.setRegion(region);
        config.setCloudStorageService(new CloudStorageService(region));
    }

    public void setBucketName(String bucketName) {
        config.setBucketName(bucketName);
    }

    public void setPasswordAttribute(String passwordAttribute) {
        config.setPasswordAttribute(passwordAttribute);
    }

    public void setPreserveOldSyncFiles(int preserverOldSyncFiles) {
        config.setPreserveOldSyncFiles(preserverOldSyncFiles);
    }

    public void setUniqueAttribute(String uniqueAttribute) {
        config.setUniqueAttribute(uniqueAttribute);
    }

    public void setNameAttribute(String nameAttribute) {
        config.setNameAttribute(nameAttribute);
    }

    public void setEncoding(String encoding) {
        config.setEncoding(encoding);
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        config.setFieldDelimiter(fieldDelimiter);
    }

    public void setEscape(String escape) {
        config.setEscape(escape);
    }

    public void setCommentMarker(String commentMarker) {
        config.setCommentMarker(commentMarker);
    }

    public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        config.setIgnoreEmptyLines(ignoreEmptyLines);
    }

    public void setQuote(String quote) {
        config.setQuote(quote);
    }

    public void setQuoteMode(String quoteMode) {
        config.setQuoteMode(quoteMode);
    }

    public void setRecordSeparator(String recordSeparator) {
        config.setRecordSeparator(recordSeparator);
    }

    public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        config.setIgnoreSurroundingSpaces(ignoreSurroundingSpaces);
    }

    public void setTrailingDelimiter(boolean trailingDelimiter) {
        config.setTrailingDelimiter(trailingDelimiter);
    }

    public void setTrim(boolean trim) {
        config.setTrim(trim);
    }
    
    public void setIgnoreIdentifierCase(boolean ignoreIdentifierCase) {
        config.setIgnoreIdentifierCase(ignoreIdentifierCase);
    }

    @Override
    public void validate() {
        LOG.info("Csv configuration validation started");

        if (objectClassDefinition != null) {
            Util.checkCanReadFile(objectClassDefinition);
        }

        try {
            List<ObjectClassHandlerConfiguration> configs = getAllConfigs();
            configs.forEach(config -> config.validate());
        } catch (IOException ex) {
            throw new ConfigurationException("Couldn't load configuration, reason: " + ex.getMessage(), ex);
        }

        LOG.info("Csv configuration validation finished");
    }

    public ObjectClassHandlerConfiguration getConfig() {
        if (config != null) {
            config.recompute();
        }

        return config;
    }

    public List<ObjectClassHandlerConfiguration> getAllConfigs() throws IOException {
        List<ObjectClassHandlerConfiguration> configs = new ArrayList<>();
        configs.add(getConfig());

        if (objectClassDefinition == null) {
            return configs;
        }

        Properties properties = new Properties();
        try (Reader r = new InputStreamReader(new FileInputStream(objectClassDefinition), Charset.forName("UTF-8"))) {
            properties.load(r);
        }

        Map<String, Map<String, Object>> ocMap = new HashMap<>();
        properties.forEach((key, value) -> {

            String strKey = (String) key;

            String oc = strKey.split("\\.")[0];
            Map<String, Object> values = ocMap.get(oc);
            if (values == null) {
                values = new HashMap<>();
                ocMap.put(oc, values);
            }

            String subKey = strKey.substring(oc.length() + 1);
            values.put(subKey, value);
        });

        ocMap.keySet().forEach(key -> {

            Map<String, Object> values = ocMap.get(key);

            ObjectClassHandlerConfiguration config = new ObjectClassHandlerConfiguration(new ObjectClass(key), values);
            config.recompute();

            configs.add(config);
        });

        return configs;
    }
}
