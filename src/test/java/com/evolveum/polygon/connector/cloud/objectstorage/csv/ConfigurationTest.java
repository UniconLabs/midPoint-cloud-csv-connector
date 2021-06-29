package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import java.io.File;
import java.io.IOException;

/**
 * Created by lazyman on 22/05/2017.
 */
public class ConfigurationTest extends BaseTest {

    @Test
    public void testS3Reading() throws Exception {
        final CloudCsvConfiguration config = createConfiguration();
        final CloudStorageService test = CloudStorageServiceFactory.getCloudServiceProvider(config);
        final Boolean exists = test.checkBucketExists(config);
        AssertJUnit.assertTrue("Bucket does not exist", exists);
    }

    @Test
    public void readOnlyMode() throws Exception {
        final CloudCsvConfiguration config = new CloudCsvConfiguration();
        final File data = new File(BaseTest.CSV_FILE_PATH);
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setReadOnly(true);

        final ConnectorFacade connector = setupConnector("/create.csv", config);

        data.setWritable(false);

        final ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        AssertJUnit.assertEquals(1, handler.getObjects().size());

        data.setWritable(true);
    }
}
