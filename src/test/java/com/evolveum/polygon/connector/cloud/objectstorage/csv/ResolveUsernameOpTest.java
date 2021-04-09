package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class ResolveUsernameOpTest extends BaseTest {

    private ConnectorFacade connector;

    @BeforeMethod
    public void before() throws Exception {
        connector = setupConnector("/authenticate.csv");
    }

    @AfterMethod
    public void after() {
        connector = null;
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() {
        connector.resolveUsername(ObjectClass.GROUP, "vilo", null);
    }

    @Test(expectedExceptions = InvalidCredentialException.class)
    public void emptyUsername() {
        connector.resolveUsername(ObjectClass.ACCOUNT, "", null);
    }

    @Test(expectedExceptions = InvalidCredentialException.class)
    public void nonExistingUsername() {
        connector.resolveUsername(ObjectClass.ACCOUNT, "unexisting", null);
    }

    @Test
    public void correctResolveUsername() {
        Uid uid = connector.resolveUsername(ObjectClass.ACCOUNT, "vilo", null);

        assertNotNull(uid);
        assertEquals(uid.getUidValue(), "vilo");
    }
}
