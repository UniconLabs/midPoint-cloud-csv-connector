package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.CsvTestUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CreateOpTest extends BaseTest {

    private static final String NEW_UID = "uid=vilo,dc=example,dc=com";
    private static final String NEW_FIRST_NAME = "viliam";
    private static final String NEW_LAST_NAME = "repan";
    private static final String NEW_PASSWORD = "asdf123";

    @Test(expectedExceptions = ConnectorException.class)
    public void readOnlyMode() throws Exception {
        CsvConfiguration config = new CsvConfiguration();

        //config.setFilePath(new File(BaseTest.CSV_FILE_PATH));
        config.setTmpFolder(new File(BaseTest.CSV_TMP_FILE_PATH));
        config.setUniqueAttribute(ATTR_UID);
        config.setPasswordAttribute(ATTR_PASSWORD);
        config.setReadOnly(true);

        ConnectorFacade connector = setupConnector("/create.csv", config);
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        connector.create(ObjectClass.ACCOUNT, attributes, null);

    }

    @Test
    public void createAccountAllAttributesNameEqualsUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = InvalidAttributeValueException.class)
    public void createAccountNameEqualsUidWrongName() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID + "different"));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountNameEqualsUidNoUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = InvalidAttributeValueException.class)
    public void createAccountNoUidAtAll() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountNameEqualsUidNoName() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(new Name(NEW_UID));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void createAccountAllAttributesDifferent() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationDifferent());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));

        connector.create(ObjectClass.ACCOUNT, attributes, null);
    }

    @Test
    public void createAccountDifferentNameNotEqualUid() throws Exception {
        ConnectorFacade connector = setupConnector("/create-empty.csv", createConfigurationDifferent());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_LAST_NAME));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_LAST_NAME));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void createAccountAllAttributesNameEqualsUidMultilineMultivalue() throws Exception {
        CsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector("/create.csv", config);

        final String SECOND_LAST_NAME = "von\nBahnhof";

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(ATTR_UID, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME, SECOND_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        Uid uid = connector.create(ObjectClass.ACCOUNT, attributes, null);
        assertNotNull(uid);
        assertEquals(NEW_UID, uid.getUidValue());

        ConnectorObject newObject = connector.getObject(ObjectClass.ACCOUNT, uid, null);
        assertNotNull(newObject);
        attributes = new HashSet<>();
        attributes.add(new Name(NEW_UID));
        attributes.add(createAttribute(Uid.NAME, NEW_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, NEW_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, NEW_LAST_NAME, SECOND_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(NEW_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, newObject);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, NEW_UID);
        expectedRecord.put(ATTR_FIRST_NAME, NEW_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, NEW_LAST_NAME + "," + SECOND_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, NEW_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), NEW_UID);
        assertEquals(expectedRecord, realRecord);
    }
}
