package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.s3.AwsS3StorageService;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.test.common.TestHelpers;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public abstract class BaseTest {

    public static final String TEMPLATE_FOLDER_PATH = "./src/test/resources";

    public static final String CSV_FILE_PATH = "./target/data.csv";
    public static final String CSV_TMP_FILE_PATH = "./target/";
    public static final String DEFAULT_S3_BUCKET_NAME = "tests3connector";
    public static final String S3_FILE_NAME = "data.csv";
    public static final String DEFAULT_S3_REGION = "us-east-1";


    public static final String ATTR_UID = "uid";
    public static final String ATTR_FIRST_NAME = "firstName";
    public static final String ATTR_LAST_NAME = "lastName";
    public static final String ATTR_PASSWORD = "password";

    CloudStorageService cloudStorageService = new AwsS3StorageService();

    protected CloudCsvConfiguration createConfiguration() {
        return createConfigurationNameEqualsUid();
    }

    protected CloudCsvConfiguration createConfigurationNameEqualsUid() {
        CloudCsvConfiguration config = null;
        try {
            config = new CloudCsvConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Properties p = new Properties();
        InputStream is = ClassLoader.getSystemResourceAsStream("app-test.properties");
        try {
            p.load(is);
            config.setRegion(p.getProperty("awstest.region"));
            config.setBucketName(p.getProperty("awstest.bucket"));
        }
        catch (IOException e) {
            config.setRegion(BaseTest.DEFAULT_S3_REGION);
            config.setBucketName(BaseTest.DEFAULT_S3_BUCKET_NAME);
        }
        config.setFileName(BaseTest.S3_FILE_NAME);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);

        return config;
    }

    protected CloudCsvConfiguration createConfigurationDifferent() {
        CloudCsvConfiguration config = null;
        try {
            config = new CloudCsvConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Properties p = new Properties();
        InputStream is = ClassLoader.getSystemResourceAsStream("app-test.properties");
        try {
            p.load(is);
            config.setRegion(p.getProperty("awstest.region"));
            config.setBucketName(p.getProperty("awstest.bucket"));
        }
        catch (IOException e) {
            config.setRegion(BaseTest.DEFAULT_S3_REGION);
            config.setBucketName(BaseTest.DEFAULT_S3_BUCKET_NAME);
        }
        config.setFileName(BaseTest.S3_FILE_NAME);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setNameAttribute(ATTR_LAST_NAME);

        return config;
    }

    protected ConnectorFacade setupConnector(String csvTemplate) throws Exception {
        return setupConnector(csvTemplate, createConfiguration());
    }

    protected ConnectorFacade setupConnector(String csvTemplate, CloudCsvConfiguration config) throws Exception {
        
    	copyDataFile(csvTemplate, config);

        return createNewInstance(config);
    }
    
    protected ConnectorFacade createNewInstance(CloudCsvConfiguration config) {
    	ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();

        APIConfiguration impl = TestHelpers.createTestConfiguration(CloudCsvObjectStorageConnector.class, config);
        return factory.newInstance(impl);
    }
    
    protected void copyDataFile(String csvTemplate, CloudCsvConfiguration config) throws Exception {
        Properties p = new Properties();
        InputStream is = ClassLoader.getSystemResourceAsStream("app-test.properties");
        try {
            p.load(is);
            config.setRegion(p.getProperty("awstest.region"));
            config.setBucketName(p.getProperty("awstest.bucket"));
        }
        catch (Exception e) {
            config.setRegion(BaseTest.DEFAULT_S3_REGION);
            config.setBucketName(BaseTest.DEFAULT_S3_BUCKET_NAME);
        }
    	File file = new File(CSV_FILE_PATH);
        file.delete();
        config.setFileName(BaseTest.S3_FILE_NAME);
        cloudStorageService.uploadFile(config, new File(TEMPLATE_FOLDER_PATH + csvTemplate));
        //config.setFilePath(new File(CSV_FILE_PATH));

        config.validate();
    }

    protected void assertConnectorObject(Set<Attribute> expected, ConnectorObject object) {
        Set<Attribute> real = object.getAttributes();
        assertEquals(expected.size(), real.size());

        for (Attribute attr : expected) {
            List<Object> expValues = attr.getValue();

            String name = attr.getName();
            Attribute realAttr = object.getAttributeByName(name);
            assertNotNull(realAttr);

            assertEquals(expValues, realAttr.getValue());
        }
    }

    protected Attribute createAttribute(String name, Object... values) {
        return AttributeBuilder.build(name, values);
    }
}
