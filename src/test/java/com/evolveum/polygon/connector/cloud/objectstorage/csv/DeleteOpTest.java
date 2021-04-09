package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertNull;

/**
 * Created by Viliam Repan (lazyman).
 */
public class DeleteOpTest extends BaseTest {

    private ConnectorFacade connector;

    @BeforeMethod
    public void beforeMethod() throws Exception {
        connector = setupConnector("/delete.csv");
    }

    @BeforeMethod
    public void afterMethod() {
        connector = null;
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void badObjectClass() {
        connector.delete(ObjectClass.GROUP, new Uid("vilo"), null);
    }

    @Test(expectedExceptions = UnknownUidException.class)
    public void notExistingUid() {
        connector.delete(ObjectClass.ACCOUNT, new Uid("unknown"), null);
    }

    @Test
    public void correctDelete() throws Exception {
        connector.delete(ObjectClass.ACCOUNT, new Uid("vilo"), null);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        AssertJUnit.assertEquals(0, handler.getObjects().size());
    }
}
