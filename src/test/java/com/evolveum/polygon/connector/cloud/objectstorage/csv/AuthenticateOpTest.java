package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import org.identityconnectors.common.Base64;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidCredentialException;
import org.identityconnectors.framework.common.exceptions.InvalidPasswordException;
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
public class AuthenticateOpTest extends BaseTest {

    private ConnectorFacade connector;

    @BeforeMethod
    public void before() throws Exception {
        connector = setupConnector("/authenticate.csv");
    }

    @AfterMethod
    public void after() {
        connector = null;
    }

    @Test(expectedExceptions = ConfigurationException.class)
    public void passwordColumNameNotDefined() throws Exception {
        CloudCsvConfiguration config = new CloudCsvConfiguration();
        config.setUniqueAttribute("uid");

        connector = setupConnector("/authenticate.csv", config);

        connector.authenticate(ObjectClass.ACCOUNT, "username",
                new GuardedString("password".toCharArray()), null);
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() {
        GuardedString guarded = new GuardedString(Base64.encode("good".getBytes()).toCharArray());
        connector.authenticate(ObjectClass.GROUP, "vilo", guarded, null);
    }

    @Test(expectedExceptions = InvalidPasswordException.class)
    public void badPassword() {
        GuardedString guarded = new GuardedString(Base64.encode("bad".getBytes()).toCharArray());
        connector.authenticate(ObjectClass.ACCOUNT, "vilo", guarded, null);
    }

    @Test(expectedExceptions = InvalidPasswordException.class)
    public void emptyPassword() {
        connector.authenticate(ObjectClass.ACCOUNT, "vilo", new GuardedString("".toCharArray()), null);
    }

    @Test(expectedExceptions = InvalidCredentialException.class)
    public void emptyUsername() {
        GuardedString guarded = new GuardedString(Base64.encode("bad".getBytes()).toCharArray());
        connector.authenticate(ObjectClass.ACCOUNT, "", guarded, null);
    }

    @Test(expectedExceptions = InvalidCredentialException.class)
    public void emptyUsernameAndPassword() {
        connector.authenticate(ObjectClass.ACCOUNT, "", new GuardedString("".toCharArray()), null);
    }

    @Test(expectedExceptions = InvalidCredentialException.class)
    public void nonExistingUsername() {
        GuardedString guarded = new GuardedString(Base64.encode("bad".getBytes()).toCharArray());
        connector.authenticate(ObjectClass.ACCOUNT, "unexisting", guarded, null);
    }

    @Test
    public void correctAuthentication() {
        GuardedString guarded = new GuardedString(Base64.encode("good".getBytes()).toCharArray());
        Uid uid = connector.authenticate(ObjectClass.ACCOUNT, "vilo", guarded, null);

        assertNotNull(uid);
        assertEquals(uid.getUidValue(), "vilo");
    }
}
