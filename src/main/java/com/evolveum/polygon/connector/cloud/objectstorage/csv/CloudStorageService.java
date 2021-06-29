package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import java.io.File;
import java.io.Reader;

/**
 * Normally this would be an interface or abstract class, but those aren't currently supported by:
 *   org.identityconnectors.framework.common.FrameworkUtil
 */
public class CloudStorageService {

    public CloudStorageService() {}

    public void uploadFile(final CloudCsvConfiguration config, File file) throws Exception {
        //do nothing
    }

    public void uploadString(final CloudCsvConfiguration config, final String stringToUpload) throws Exception {
        //do nothing
    }

    public void getFileAsAFile(final CloudCsvConfiguration config, final File fileToCopyTo) throws Exception {
        //do nothing
    }

    public Object getFileLastUpdated(final CloudCsvConfiguration config) throws Exception {
        return null;
    }

    public boolean checkBucketExists(final CloudCsvConfiguration config) throws Exception {
        return false;
    }

    public boolean checkFileExistsAndCanRead(final CloudCsvConfiguration config) {
        return Util.checkCanReadFile(new File(config.getFileName()));
    }

    public Reader getFileAsReader(final CloudCsvConfiguration config) throws Exception {
        return Util.createReader(new File(config.getFileName()), config);
    }

    public void createBucketIfNotExists(CloudCsvConfiguration configuration) throws Exception {
        //do nothing
    }
}
