package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.CsvTestUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class UpdateOpTest extends BaseTest {

    public static final String TEMPLATE_UPDATE = "/update.csv";

    public static final String VILO_UID = "vilo";
    public static final String VILO_LAST_NAME = "repan";
    public static final String VILO_FIRST_NAME = "viliam";
    public static final String VILO_PASSWORD = "Z29vZA==";

    public static final String CHANGED_VALUE = "repantest";

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        connector.update(ObjectClass.GROUP, new Uid(VILO_UID), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = UnknownUidException.class)
    public void notExistingUid() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        connector.update(ObjectClass.ACCOUNT, new Uid("unknown"), new HashSet<Attribute>(), null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void updateNonExistingAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build("nonExisting", CHANGED_VALUE));
        connector.update(ObjectClass.ACCOUNT, new Uid(VILO_UID), attributes, null);
    }

    /**
     * uid column is not editable
     *
     * @throws Exception
     */
    @Test
    public void updateAttributeUid() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(new Uid(CHANGED_VALUE), real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, CHANGED_VALUE));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, CHANGED_VALUE);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), CHANGED_VALUE);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateNameAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(Name.NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, new Uid(VILO_UID), attributes, null);

        AssertJUnit.assertEquals(new Uid(CHANGED_VALUE), real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, CHANGED_VALUE));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, CHANGED_VALUE);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), CHANGED_VALUE);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateDifferentNameAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationDifferent());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(Name.NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(CHANGED_VALUE));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void updateOtherAttribute() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, CHANGED_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void updateUidAttributeWithTwoValues() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, "first", "second"));
        connector.update(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void updateDifferentOtherAttributeMultivalueNotDefined() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationDifferent());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_FIRST_NAME, CHANGED_VALUE, "second value"));
        connector.update(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test
    public void updateDifferentOtherAttributeMultivalue() throws Exception {
        CsvConfiguration config = createConfigurationDifferent();
        config.setMultivalueDelimiter(",");

        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        final String SECOND_VALUE = "second value";

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_FIRST_NAME, CHANGED_VALUE, SECOND_VALUE));
        Uid real = connector.update(ObjectClass.ACCOUNT, expected, attributes, null);

        assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_LAST_NAME));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, CHANGED_VALUE, SECOND_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, CHANGED_VALUE + "," + SECOND_VALUE);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME );
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }
}
