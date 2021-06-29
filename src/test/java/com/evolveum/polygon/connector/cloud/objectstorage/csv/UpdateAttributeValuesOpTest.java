package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.CsvTestUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
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
public class UpdateAttributeValuesOpTest extends UpdateOpTest {

    @Test(expectedExceptions = ConnectorException.class)
    public void addValueToAttributeDelimiterUndefined() throws Exception {
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, createConfigurationNameEqualsUid());

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test
    public void addValueToAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, CHANGED_VALUE));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME, CHANGED_VALUE));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME + "," + CHANGED_VALUE);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void addDuplicateValueToAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, VILO_LAST_NAME));
        Uid real = connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeValueFromAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, VILO_LAST_NAME));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, "");
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test
    public void removeNonExistingValueFromAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_LAST_NAME, "unexisting value"));
        Uid real = connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);

        AssertJUnit.assertEquals(expected, real);

        ConnectorObject object = connector.getObject(ObjectClass.ACCOUNT, real, null);
        assertNotNull(object);

        attributes = new HashSet<>();
        attributes.add(new Name(VILO_UID));
        attributes.add(createAttribute(Uid.NAME, VILO_UID));
        attributes.add(createAttribute(ATTR_FIRST_NAME, VILO_FIRST_NAME));
        attributes.add(createAttribute(ATTR_LAST_NAME, VILO_LAST_NAME));
        attributes.add(AttributeBuilder.buildPassword(new GuardedString(VILO_PASSWORD.toCharArray())));
        assertConnectorObject(attributes, object);

        Map<String, String> expectedRecord = new HashMap<>();
        expectedRecord.put(ATTR_UID, VILO_UID);
        expectedRecord.put(ATTR_FIRST_NAME, VILO_FIRST_NAME);
        expectedRecord.put(ATTR_LAST_NAME, VILO_LAST_NAME);
        expectedRecord.put(ATTR_PASSWORD, VILO_PASSWORD);

        Map<String, String> realRecord = CsvTestUtil.findRecord(createConfigurationNameEqualsUid(), VILO_UID);
        assertEquals(expectedRecord, realRecord);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void addValueFromUniqueAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, "second value"));
        connector.addAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void removeValueFromUniqueAttribute() throws Exception {
        CloudCsvConfiguration config = createConfigurationNameEqualsUid();
        config.setMultivalueDelimiter(",");
        ConnectorFacade connector = setupConnector(TEMPLATE_UPDATE, config);

        Uid expected = new Uid(VILO_UID);

        Set<Attribute> attributes = new HashSet<>();
        attributes.add(AttributeBuilder.build(ATTR_UID, VILO_UID));
        connector.removeAttributeValues(ObjectClass.ACCOUNT, expected, attributes, null);
    }
}
