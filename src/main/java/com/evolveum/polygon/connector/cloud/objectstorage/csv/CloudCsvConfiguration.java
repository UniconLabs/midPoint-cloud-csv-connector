package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CloudCsvConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(CloudCsvConfiguration.class);

    private final ObjectClass objectClass;

    private String objectClassDefinition;
    private String cloudObjectStorageProvider;
    private String fileName;
    private String bucketName;
    private String region;
    private String endpoint;
    private boolean readOnly = false;
    private String uniqueAttribute;
    private String nameAttribute;
    private String passwordAttribute;
    private boolean ignoreIdentifierCase = false;
    private String encoding;
    private boolean headerExists = true;
    private String fieldDelimiter;
    private String escape;
    private String commentMarker;
    private boolean ignoreEmptyLines = true;
    private String quote;
    private String quoteMode;
    private String multivalueDelimiter;
    private String recordSeparator;
    private boolean trim = false;
    private boolean ignoreSurroundingSpaces = false;
    private boolean trailingDelimiter = false;
    private String multivalueAttributes;
    private boolean container = false;
    private boolean auxiliary = false;
    private String accessKey;
    private GuardedString secretKey;
    private String profileName;
    private String profilePath;
    private boolean testMode;


    public CloudCsvConfiguration() throws Exception {
        this(ObjectClass.ACCOUNT, null);
    }

    public CloudCsvConfiguration(final ObjectClass oc, final Map<String, Object> values) throws Exception {
        this.objectClass = oc;

        setCloudObjectStorageProvider(Util.getSafeValue(values, "cloudObjectStorageProvider", "s3", String.class));
        setRegion(Util.getSafeValue(values, "region", null));
        setBucketName(Util.getSafeValue(values, "bucketName", null));
        setFileName(Util.getSafeValue(values, "fileName", null));
        setAccessKey(Util.getSafeValue(values, "accessKey", null));
        setSecretKey(Util.getSafeValue(values, "secretKey", null));
        setProfileName(Util.getSafeValue(values, "profileName", null));
        setProfilePath(Util.getSafeValue(values, "profilePath", null));
        setTestMode(Util.getSafeValue(values, "testMode", false, Boolean.class));

        setEncoding(Util.getSafeValue(values, "encoding", "utf-8"));
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
        setReadOnly(Util.getSafeValue(values, "readOnly", false, Boolean.class));
        setIgnoreIdentifierCase(Util.getSafeValue(values, "ignoreIdentifierCase", false, Boolean.class));
        setContainer(Util.getSafeValue(values, "container", false, Boolean.class));
        setAuxiliary(Util.getSafeValue(values, "auxiliary", false, Boolean.class));
    }


    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "UI_CSV_REMOTE_PROVIDER",
            helpMessageKey = "UI_CSV_REMOTE_PROVIDER_HELP", required = true)
    public String getCloudObjectStorageProvider() {
        return cloudObjectStorageProvider;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "UI_CSV_FILE_NAME",
            helpMessageKey = "UI_CSV_FILE_NAME_HELP", required = true)
    public String getFileName() {
        return fileName;
    }

    @ConfigurationProperty(
            order = 3,
            displayMessageKey = "UI_CSV_BUCKET_NAME",
            helpMessageKey = "UI_CSV_BUCKET_NAME_HELP", required = true)
    public String getBucketName() {
        return bucketName;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "UI_CSV_REGION_NAME",
            helpMessageKey = "UI_CSV_REGION_NAME_HELP", required = false)
    public String getRegion() {
        return region;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "UI_CSV_ENDPOINT_NAME",
            helpMessageKey = "UI_CSV_ENDPOINT_NAME_HELP", required = false)
    public String getEndpoint() {
        return endpoint;
    }

    @ConfigurationProperty(
            order = 10,
            displayMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE",
            helpMessageKey = "UI_CSV_UNIQUE_ATTRIBUTE_HELP", required = true)
    public String getUniqueAttribute() {
        return uniqueAttribute;
    }

    @ConfigurationProperty(
            order = 6,
            displayMessageKey = "UI_CSV_ACCESS_KEY",
            helpMessageKey = "UI_CSV_ACCESS_KEY_HELP")
    public String getAccessKey() {
        return accessKey;
    }

    @ConfigurationProperty(
            order = 7,
            displayMessageKey = "UI_CSV_SECRET_KEY",
            helpMessageKey = "UI_CSV_SECRET_KEY_HELP",
            confidential = true)
    public GuardedString getSecretKey() {
        return secretKey;
    }

    public String getSecretKeyPlain() {
        if (secretKey != null) {
            final StringBuilder plain = new StringBuilder();
            secretKey.access(clearChars -> plain.append(new String(clearChars)));
            return plain.toString();
        }
        return null;
    }

    @ConfigurationProperty(
            order = 8,
            displayMessageKey = "UI_CSV_PROFILE_NAME",
            helpMessageKey = "UI_CSV_PROFILE_NAME_HELP")
    public String getProfileName() {
        return profileName;
    }

    @ConfigurationProperty(
            order = 9,
            displayMessageKey = "UI_CSV_PROFILE_PATH",
            helpMessageKey = "UI_CSV_PROFILE_PATH_HELP")
    public String getProfilePath() {
        return profilePath;
    }

    @ConfigurationProperty(
            order = 11,
            displayMessageKey = "UI_CSV_NAME_ATTRIBUTE",
            helpMessageKey = "UI_CSV_NAME_ATTRIBUTE_HELP")
    public String getNameAttribute() {
        return nameAttribute;
    }

    @ConfigurationProperty(
            order = 12,
            displayMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE",
            helpMessageKey = "UI_CSV_PASSWORD_ATTRIBUTE_HELP")
    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    @ConfigurationProperty(
            order = 13,
            displayMessageKey = "UI_CSV_ENCODING",
            helpMessageKey = "UI_CSV_ENCODING_HELP")
    public String getEncoding() {
        return encoding;
    }

    @ConfigurationProperty(
            order = 14,
            displayMessageKey = "UI_CSV_FIELD_DELIMITER",
            helpMessageKey = "UI_CSV_FIELD_DELIMITER_HELP")
    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    @ConfigurationProperty(
            order = 15,
            displayMessageKey = "UI_CSV_ESCAPE",
            helpMessageKey = "UI_CSV_ESCAPE_HELP")
    public String getEscape() {
        return escape;
    }

    @ConfigurationProperty(
            order = 17,
            displayMessageKey = "UI_CSV_COMMENT_MARKER",
            helpMessageKey = "UI_CSV_COMMENT_MARKER_HELP")
    public String getCommentMarker() {
        return commentMarker;
    }

    @ConfigurationProperty(
            order = 18,
            displayMessageKey = "UI_CSV_IGNORE_EMPTY_LINES",
            helpMessageKey = "UI_CSV_IGNORE_EMPTY_LINES_HELP")
    public boolean isIgnoreEmptyLines() {
        return ignoreEmptyLines;
    }

    @ConfigurationProperty(
            order = 19,
            displayMessageKey = "UI_CSV_QUOTE",
            helpMessageKey = "UI_CSV_QUOTE_HELP")
    public String getQuote() {
        return quote;
    }

    @ConfigurationProperty(
            order = 20,
            displayMessageKey = "UI_CSV_QUOTE_MODE",
            helpMessageKey = "UI_CSV_QUOTE_MODE_HELP")
    public String getQuoteMode() {
        return quoteMode;
    }

    @ConfigurationProperty(
            order = 21,
            displayMessageKey = "UI_CSV_RECORD_SEPARATOR",
            helpMessageKey = "UI_CSV_RECORD_SEPARATOR_HELP")
    public String getRecordSeparator() {
        return recordSeparator;
    }

    @ConfigurationProperty(
            order = 22,
            displayMessageKey = "UI_CSV_IGNORE_SURROUNDING_SPACES",
            helpMessageKey = "UI_CSV_IGNORE_SURROUDING_SPACES_HELP")
    public boolean isIgnoreSurroundingSpaces() {
        return ignoreSurroundingSpaces;
    }

    @ConfigurationProperty(
            order = 23,
            displayMessageKey = "UI_CSV_TRAILING_DELIMITER",
            helpMessageKey = "UI_CSV_TRAILING_DELIMITER_HELP")
    public boolean isTrailingDelimiter() {
        return trailingDelimiter;
    }

    @ConfigurationProperty(
            order = 24,
            displayMessageKey = "UI_CSV_TRIM",
            helpMessageKey = "UI_CSV_TRIM_HELP")
    public boolean isTrim() {
        return trim;
    }

    @ConfigurationProperty(
            order = 25,
            displayMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER",
            helpMessageKey = "UI_CSV_MULTI_VALUE_DELIMITER_HELP")
    public String getMultivalueDelimiter() {
        return multivalueDelimiter;
    }

    @ConfigurationProperty(
            order = 26,
            displayMessageKey = "UI_CSV_OBJECT_CLASS_DEFINITION",
            helpMessageKey = "UI_CSV_OBJECT_CLASS_DEFINITION_HELP")
    public String getObjectClassDefinition() {
        return objectClassDefinition;
    }

    @ConfigurationProperty(
            order = 27,
            displayMessageKey = "UI_CSV_HEADER_EXISTS",
            helpMessageKey = "UI_CSV_HEADER_EXISTS_HELP")
    public boolean isHeaderExists() {
        return headerExists;
    }

    @ConfigurationProperty(
            order = 28,
            displayMessageKey = "UI_CSV_READ_ONLY",
            helpMessageKey = "UI_CSV_READ_ONLY_HELP")
    public boolean isReadOnly() {
        return readOnly;
    }

    @ConfigurationProperty(
            order = 29,
            displayMessageKey = "UI_IGNORE_IDENTIFIER_CASE",
            helpMessageKey = "UI_IGNORE_IDENTIFIER_CASE_HELP")
    public boolean isIgnoreIdentifierCase() {
        return ignoreIdentifierCase;
    }

    @ConfigurationProperty(
            order = 30,
            displayMessageKey = "UI_MULTIVALUE_ATTRIBUTES",
            helpMessageKey = "UI_MULTIVALUE_ATTRIBUTES_HELP")
    public String getMultivalueAttributes() {
        return multivalueAttributes;
    }

    @ConfigurationProperty(
            order = 31,
            displayMessageKey = "UI_CONTAINER",
            helpMessageKey = "UI_CONTAINER_HELP")
    public boolean isContainer() {
        return container;
    }

    @ConfigurationProperty(
            order = 32,
            displayMessageKey = "UI_AUXILIARY",
            helpMessageKey = "UI_AUXILIARY_HELP")
    public boolean isAuxiliary() {
        return auxiliary;
    }

    @ConfigurationProperty(
            order = 33,
            displayMessageKey = "UI_TEST_MODE",
            helpMessageKey = "UI_TEST_MODE_HELP")
    public boolean isTestMode() {
        return testMode;
    }

    public ObjectClass getObjectClass() {
        return objectClass;
    }


    public void setUniqueAttribute(final String uniqueAttribute) {
        this.uniqueAttribute = uniqueAttribute;

        if (this.getNameAttribute() == null) {
            this.setNameAttribute(uniqueAttribute);
        }
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public void setNameAttribute(String nameAttribute) {
        if (StringUtil.isEmpty(nameAttribute) && StringUtil.isNotEmpty(uniqueAttribute)) {
            this.nameAttribute = uniqueAttribute;
        } else {
            this.nameAttribute = nameAttribute;
        }
    }

    public void setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
    }

    public void setIgnoreIdentifierCase(boolean ignoreIdentifierCase) {
        this.ignoreIdentifierCase = ignoreIdentifierCase;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setHeaderExists(boolean headerExists) {
        this.headerExists = headerExists;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public void setCommentMarker(String commentMarker) {
        this.commentMarker = commentMarker;
    }

    public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
        this.ignoreEmptyLines = ignoreEmptyLines;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    public void setQuoteMode(String quoteMode) {
        this.quoteMode = quoteMode;
    }

    public void setMultivalueDelimiter(String multivalueDelimiter) {
        this.multivalueDelimiter = multivalueDelimiter;
    }

    public void setRecordSeparator(String recordSeparator) {
        this.recordSeparator = recordSeparator;
    }

    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
        this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
    }

    public void setTrailingDelimiter(boolean trailingDelimiter) {
        this.trailingDelimiter = trailingDelimiter;
    }

    public void setMultivalueAttributes(String multivalueAttributes) {
        this.multivalueAttributes = multivalueAttributes;
    }

    public void setContainer(boolean container) {
        this.container = container;
    }

    public void setAuxiliary(boolean auxiliary) {
        this.auxiliary = auxiliary;
    }

    public void setObjectClassDefinition(final String objectClassDefinition) {
        this.objectClassDefinition = objectClassDefinition;
    }

    public void setCloudObjectStorageProvider(final String cloudObjectStorageProvider) {
        this.cloudObjectStorageProvider = cloudObjectStorageProvider;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setSecretKey(GuardedString secretKey) {
        this.secretKey = secretKey;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    public void setProfilePath(String profilePath) {
        this.profilePath = profilePath;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }


    public void validate() {
        LOG.ok("Validating configuration for {0}", objectClass);
        Util.notEmpty(getEncoding(), "Encoding is not defined.");

        if (!Charset.isSupported(getEncoding())) {
            throw new ConfigurationException("Encoding '" + getEncoding() + "' is not supported");
        }

        Util.notEmpty(getFieldDelimiter(), "Field delimiter can't be null or empty");
        Util.notEmpty(getEscape(), "Escape character is not defined");
        Util.notEmpty(getCommentMarker(), "Comment marker character is not defined");
        Util.notEmpty(getQuote(), "Quote character is not defined");

        Util.notEmpty(getQuoteMode(), "Quote mode is not defined");
        boolean found = false;
        for (QuoteMode qm : QuoteMode.values()) {
            if (qm.name().equalsIgnoreCase(getQuoteMode())) {
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

            throw new ConfigurationException("Quote mode '" + getQuoteMode() + "' is not supported, supported values: ["
                    + sb + "]");
        }

        Util.notEmpty(getRecordSeparator(), "Record separator is not defined");

        validateAttributeNames();
    }

    public List<CloudCsvConfiguration> getAllConfigs() throws Exception {
        List<CloudCsvConfiguration> configs = new ArrayList<>();
        configs.add(this);

        if (StringUtil.isNotEmpty(objectClassDefinition)) {
            return configs;
        }

        final Properties properties = new Properties();
        if (StringUtil.isNotBlank(this.getObjectClassDefinition())) {
            try (Reader r = new InputStreamReader(new FileInputStream(this.getObjectClassDefinition()), StandardCharsets.UTF_8)) {
                properties.load(r);
            } catch (Exception e) {
                LOG.info("Exception loading Object Class Definition File {}", e);
                throw e;
            }
        }

        final Map<String, Map<String, Object>> ocMap = new HashMap<>();
        properties.forEach((key, value) -> {
            final String strKey = (String) key;

            final String oc = strKey.split("\\.")[0];
            Map<String, Object> values = ocMap.computeIfAbsent(oc, k -> new HashMap<>());

            final String subKey = strKey.substring(oc.length() + 1);
            values.put(subKey, value);
        });

        ocMap.keySet().forEach(key -> {
            final Map<String, Object> values = ocMap.get(key);
            CloudCsvConfiguration config = null;
            try {
                config = new CloudCsvConfiguration(new ObjectClass(key), values);
            } catch (Exception e) {
                e.printStackTrace();
            }

            configs.add(config);
        });

        return configs;
    }

    private void validateAttributeNames() {
        if (StringUtil.isEmpty(uniqueAttribute)) {
            throw new ConfigurationException("Unique attribute is not defined.");
        }

        if (StringUtil.isEmpty(getNameAttribute())) {
            LOG.ok("Name attribute not defined, value from unique attribute will be used (" + uniqueAttribute + ").");
            setNameAttribute(uniqueAttribute);
        }

        if (StringUtil.isEmpty(getPasswordAttribute())) {
            LOG.ok("Password attribute is not defined.");
        }
    }
}
