package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.BaseTest;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.CsvConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.logging.Log;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * Created by Viliam Repan (lazyman).
 */
public class CsvTestUtil {

    private static final Log LOG = Log.getLog(CsvTestUtil.class);

    public static Map<String, String> findRecord(CsvConfiguration config, String value) throws IOException {
        return findRecord(config, BaseTest.ATTR_UID, value);
    }

    public static Map<String, String> findRecord(CsvConfiguration config, String uniqueColumn, String value)
            throws IOException {
        Util.notNull(config, "CsvConfiguration must not be null");
        Util.notEmpty(uniqueColumn, "Unique column name must not be empty");

        CSVFormat csv = Util.createCsvFormat(config.getConfig()).withFirstRecordAsHeader();

        try (Reader reader = Util.createReader(config)) {

            CSVParser parser = csv.parse(reader);
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                Map<String, String> map = record.toMap();

                String unique = map.get(uniqueColumn);
                if (Objects.equals(value, unique)) {
                    return map;
                }
            }
        }

        return null;
    }

    public static void deleteAllSyncFiles() throws IOException {
        File target = new File("./target");

        File[] list = target.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                if (name.matches("data\\.csv\\.sync\\.\\d{13}")) {
                    return true;
                }

                return false;
            }
        });

        for (File file : list) {
            LOG.info("Deleting {0}", file.getName());
            file.delete();
        }
    }
}
