package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.ListResultHandler;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

/**
 * Created by Viliam Repan (lazyman).
 */
public class SearchOpTest extends BaseTest {

    @Test
    public void findAllAccounts() throws Exception {
        ConnectorFacade connector = setupConnector("/search.csv");

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());
    }

    @Test
    public void findOneWithRepeatingColumn() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        //config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        ListResultHandler handler = new ListResultHandler();
        EqualsFilter ef = new EqualsFilter(new Uid("1"));
        connector.search(ObjectClass.ACCOUNT, ef, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(1, objects.size());

        //todo asserts
    }

    @Test
    public void findAllAccountsRepeatingColumn() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        //config.setFilePath(new File(CSV_FILE_PATH));
        config.setUniqueAttribute("id");
        config.setTrim(true);
        ConnectorFacade connector = setupConnector("/schema-repeating-column.csv", config);

        ListResultHandler handler = new ListResultHandler();
        connector.search(ObjectClass.ACCOUNT, null, handler, null);

        List<ConnectorObject> objects = handler.getObjects();
        AssertJUnit.assertEquals(2, objects.size());

        //todo asserts
    }

    @Test(expectedExceptions = ConnectorException.class)
    public void searchWrongNumberColumnCountInRow() throws Exception {
        CsvConfiguration config = new CsvConfiguration();
        //config.setFilePath(new File(CSV_FILE_PATH));
        config.setQuoteMode("ALL");
        config.setFieldDelimiter(",");
        config.setMultivalueDelimiter(";");
        config.setUniqueAttribute("login");
        config.setPasswordAttribute("password");

        ConnectorFacade connector = setupConnector("/search-wrong-column-count-row.csv", config);
        connector.search(ObjectClass.ACCOUNT, null, new ListResultHandler(), null);
    }
}
