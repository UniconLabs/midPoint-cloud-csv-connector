package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.annotations.Test;
import java.io.File;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import static org.testng.AssertJUnit.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SchemaOpTest extends BaseTest {

    @Test
    public void multipleClasses() throws Exception {
        final CloudCsvConfiguration config = createConfiguration();
                config.setUniqueAttribute("id");
        config.setTrim(true);
        config.setPasswordAttribute(null);
        final CloudStorageService test = CloudStorageServiceFactory.getCloudServiceProvider(config);

        final File groupsProperties = new File("./target/groups.properties");
        groupsProperties.delete();
        config.setObjectClassDefinition("./target/groups.properties");

        FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups.properties"), groupsProperties);
        final File groupsCsv = new File("./target/groups.csv");
        groupsCsv.delete();

        //FileUtils.copyFile(new File(TEMPLATE_FOLDER_PATH + "/groups.csv"), groupsCsv);
        test.uploadFile(config, new File(TEMPLATE_FOLDER_PATH + "/groups.csv"));

        final ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        final Schema schema = connector.schema();
        assertEquals(2, schema.getObjectClassInfo().size());

        final ObjectClassInfo info = schema.findObjectClassInfo("group");
        assertNotNull(info);
    }

    @Test
    public void repeatingColumns() throws Exception {
        final CloudCsvConfiguration config = createConfiguration();
        config.setUniqueAttribute("id");
        config.setTrim(true);
        config.setPasswordAttribute(null);

        final ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);
        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void emptySchema() throws Exception {
        final ConnectorFacade connector = setupConnector("/schema-empty.csv");
        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void badPwdFileSchema() throws Exception {
        final ConnectorFacade connector = setupConnector("/schema-bad-pwd.csv");
        connector.schema();
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void badUniqueFileSchema() throws Exception {
        final CloudCsvConfiguration config = new CloudCsvConfiguration();
        config.setUniqueAttribute("uid");

        final ConnectorFacade connector = setupConnector("/schema-bad-unique.csv", config);
        connector.schema();
    }

    @Test
    public void goodFileSchema() throws Exception {
        final ConnectorFacade connector = setupConnector("/schema-good.csv");

        final Schema schema = connector.schema();
        assertNotNull(schema);
        final Set<ObjectClassInfo> objClassInfos = schema.getObjectClassInfo();
        assertNotNull(objClassInfos);
        assertEquals(1, objClassInfos.size());

        final ObjectClassInfo info = objClassInfos.iterator().next();
        assertNotNull(info);
        assertEquals(ObjectClass.ACCOUNT.getObjectClassValue(), info.getType());
        assertFalse(info.isContainer());
        final Set<AttributeInfo> attrInfos = info.getAttributeInfo();
        assertNotNull(attrInfos);
        assertEquals(5, attrInfos.size());

        testAttribute(Uid.NAME, attrInfos, "uid");
        testAttribute("firstName", attrInfos, "firstName");
        testAttribute("lastName", attrInfos, "lastName");
        testAttribute(Name.NAME, attrInfos, "uid");
        testAttribute(OperationalAttributes.PASSWORD_NAME, attrInfos, "password", true);
    }

    @Test
    public void uniqueDifferentThanNameSchema() throws Exception {
        final CloudCsvConfiguration config = new CloudCsvConfiguration();
        config.setUniqueAttribute("uid");
        config.setNameAttribute("lastName");
        config.setPasswordAttribute("password");

        final ConnectorFacade connector = setupConnector("/schema-good.csv", config);

        final Schema schema = connector.schema();
        assertNotNull(schema);
        final Set<ObjectClassInfo> objClassInfos = schema.getObjectClassInfo();
        assertNotNull(objClassInfos);
        assertEquals(1, objClassInfos.size());

        final ObjectClassInfo info = objClassInfos.iterator().next();
        assertNotNull(info);
        assertEquals(ObjectClass.ACCOUNT.getObjectClassValue(), info.getType());
        assertFalse(info.isContainer());
        final Set<AttributeInfo> attrInfos = info.getAttributeInfo();
        assertNotNull(attrInfos);
        assertEquals(5, attrInfos.size());

        testAttribute("firstName", attrInfos, "firstName");
        testAttribute("uid", attrInfos, "uid");
        testAttribute(Uid.NAME, attrInfos, "uid");
        testAttribute(Name.NAME, attrInfos, "lastName");
        testAttribute(OperationalAttributes.PASSWORD_NAME, attrInfos, "password", true);
    }

    private void testAttribute(String name, Set<AttributeInfo> attrInfos, String nativeName) {
        testAttribute(name, attrInfos, nativeName, false);
    }

    private void testAttribute(String name, Set<AttributeInfo> attrInfos, String nativeName, boolean password) {
        final Iterator<AttributeInfo> iterator = attrInfos.iterator();

        boolean found = false;
        while (iterator.hasNext()) {
            final AttributeInfo info = iterator.next();
            assertNotNull(info);

            if (!name.equals(info.getName())) {
                continue;
            }
            found = true;

            if (password) {
                assertEquals(GuardedString.class, info.getType());
            } else {
                assertEquals(String.class, info.getType());
            }

            assertEquals(nativeName, info.getNativeName());
        }

        assertTrue(found);
    }
}
