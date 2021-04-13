package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.CsvTestUtil;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.S3Utils;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import java.io.File;
import java.io.IOException;
import java.util.*;
import org.apache.commons.io.FileUtils;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SyncOpTest extends BaseTest {

    private static final Log LOG = Log.getLog(SyncOpTest.class);

    private CloudStorageService cloudStorageService= new CloudStorageService();

    @Test(expectedExceptions = ConnectorException.class)
    public void syncLock() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync.csv.1300734815289"), oldSyncFile);

        File lock = new File("./target/data.csv." + Util.SYNC_LOCK_EXTENSION);
        lock.createNewFile();

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> true, null);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
            lock.delete();
        }
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badHeaders() throws Exception {
        ConnectorFacade connector = setupConnector("/sync-bad.csv", createConfiguration());

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync-bad.csv.1300734815289"), oldSyncFile);

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> {
                Assert.fail("This test should fail on headers check.");
                return false;
            }, null);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }

        Assert.fail("This test should fail on headers check.");
    }

    @Test
    public void syncTest() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        File oldSyncFile = new File("./target/data.csv.sync.1300734815289");
        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, "sync.csv.1300734815289"), oldSyncFile);

        try {
//            SyncToken oldToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
//            assertEquals("1300734815289", oldToken.getValue());

            final List<SyncDelta> deltas = new ArrayList<>();
            connector.sync(ObjectClass.ACCOUNT, new SyncToken("1300734815289"), delta -> {
                deltas.add(delta);
                return true;
            }, null);

            AssertJUnit.assertEquals(3, deltas.size());

            SyncToken token = deltas.get(0).getToken();

            Map<String, SyncDelta> deltaMap = createSyncDeltaTestMap(token);
            for (SyncDelta delta : deltas) {
                SyncDelta syncDelta = deltaMap.get(delta.getUid().getUidValue());
                deltaMap.remove(delta.getUid().getUidValue());
                assertEquals(syncDelta, delta);
            }
            assertTrue(deltaMap.isEmpty(), "deltas didn't match");
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    @Test
    public void syncActualTokenTest() throws Exception {
        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync.csv", config);

        try {
            long timestampBefore = System.currentTimeMillis();
            SyncToken token = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            long timestampToken = Long.valueOf((String) token.getValue());
            long timestampAfter = System.currentTimeMillis();

            assertTrue(timestampToken>timestampBefore && timestampToken<timestampAfter, "wrong token, expected token between "+timestampBefore+" and "+timestampAfter);
        } finally {
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    private Map<String, SyncDelta> createSyncDeltaTestMap(SyncToken token) {
        Map<String, SyncDelta> map = new HashMap<String, SyncDelta>();

        SyncDeltaBuilder builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.DELETE);
        builder.setObjectClass(ObjectClass.ACCOUNT);
        builder.setToken(token);
        builder.setUid(new Uid("vilo"));

        ConnectorObjectBuilder cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("vilo");
        cBuilder.setUid("vilo");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "viliam");
        cBuilder.addAttribute(ATTR_LAST_NAME, "repan");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));
        builder.setObject(cBuilder.build());

        map.put("vilo", builder.build());

        // =====================

        builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.UPDATE);
        builder.setToken(token);
        builder.setUid(new Uid("miso"));

        cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("miso");
        cBuilder.setUid("miso");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "michal");
        cBuilder.addAttribute(ATTR_LAST_NAME, "LastnameChange");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));

        builder.setObject(cBuilder.build());
        map.put("miso", builder.build());

        // =====================

        builder = new SyncDeltaBuilder();
        builder.setDeltaType(SyncDeltaType.CREATE);
        builder.setToken(token);
        builder.setUid(new Uid("apple"));

        cBuilder = new ConnectorObjectBuilder();
        cBuilder.setName("apple");
        cBuilder.setUid("apple");
        cBuilder.setObjectClass(ObjectClass.ACCOUNT);
        cBuilder.addAttribute(ATTR_FIRST_NAME, "small");
        cBuilder.addAttribute(ATTR_LAST_NAME, "smallAppleChange");
        cBuilder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString("Z29vZA==".toCharArray()));

        builder.setObject(cBuilder.build());
        map.put("apple", builder.build());

        return map;
    }

    @Test
    public void loopSync() throws Exception {
        final long SYNC_WAIT_TIME = 200;
        final long RUN_TIME = 15 * 1000;

        CsvTestUtil.deleteAllSyncFiles();

        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync-loop-1.csv", config);

        int runCount = 0;
        try {
            SyncToken startToken = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
            switchCsvFile(true, config);

            long oldTokenValue;
            boolean useSecond = false;

            SyncToken token = startToken;
            long startTime = System.currentTimeMillis();
            while (startTime + RUN_TIME > System.currentTimeMillis()) {
                runCount++;

                oldTokenValue = Long.parseLong((String) token.getValue());

                final SyncToken[] tokenHolder = new SyncToken[1];

                final List<SyncDelta> deltas = new ArrayList<>();
                connector.sync(ObjectClass.ACCOUNT, token, new SyncTokenResultsHandler() {

                    @Override
                    public void handleResult(SyncToken syncToken) {
                        tokenHolder[0] = syncToken;
                    }

                    @Override
                    public boolean handle(SyncDelta delta) {
                        deltas.add(delta);
                        return true;
                    }
                }, null);

                Thread.sleep(SYNC_WAIT_TIME);

                AssertJUnit.assertEquals(4, deltas.size());

                SyncToken newToken = tokenHolder[0];
                if (newToken == null && !deltas.isEmpty()) {
                    newToken = deltas.get(0).getToken();
                } else {
                    newToken = startToken;
                }

                long newTokenValue = Long.parseLong((String) newToken.getValue());
                AssertJUnit.assertTrue(newTokenValue > oldTokenValue);

                token = newToken;

                switchCsvFile(useSecond, config);
                useSecond = !useSecond;
            }
        } finally {
            LOG.info("Run count: {0}", runCount);
            CsvTestUtil.deleteAllSyncFiles();
        }
    }

    private void switchCsvFile(boolean useSecond, CsvConfiguration config) throws IOException {
        String file = useSecond ? "sync-loop-2.csv" : "sync-loop-1.csv";
        cloudStorageService.uploadFileToS3(config.getBucketName(), config.getFileName(), new File(TEMPLATE_FOLDER_PATH + "/" + file));
        //File csv = new File(CSV_FILE_PATH);
        //FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH, file), csv);
        //FileUtils.touch(csv);

        LOG.info("Using second={0}, time: {1}", useSecond, cloudStorageService.getObjectLastUpdated(config.getBucketName(), config.getFileName()));
    }

    @Test
    public void syncTokenTest() throws Exception {
        CsvTestUtil.deleteAllSyncFiles();

        CsvConfiguration config = createConfiguration();
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/sync-loop-1.csv", config);

        SyncToken token = connector.getLatestSyncToken(ObjectClass.ACCOUNT);
        switchCsvFile(true, config);

        int count = 0;
        SyncToken token2 = null;
        while (count < 15) {
            count++;

            token2 = doSync(connector, token);
            switchCsvFile(false, config);

            File toDelete = Util.createSyncFileName(Long.parseLong((String) token2.getValue()), config.getConfig());
            if (!toDelete.delete()) {
                throw new RuntimeException("Couldn't delete " + toDelete.getName());
            }

            doSync(connector, token2);
        }
    }

    private SyncToken doSync(ConnectorFacade connector, SyncToken token) throws Exception {
        final List<SyncDelta> deltas = new ArrayList<>();

        SyncToken newToken = connector.sync(ObjectClass.ACCOUNT, token, new SyncTokenResultsHandler() {

            @Override
            public void handleResult(SyncToken syncToken) {
            }

            @Override
            public boolean handle(SyncDelta delta) {
                deltas.add(delta);
                return true;
            }
        }, null);

        Thread.sleep(200);

        if (newToken == null && !deltas.isEmpty()) {
            newToken = deltas.get(0).getToken();
        }

        LOG.info("New token is {0}", newToken);

        return newToken;
    }
}
