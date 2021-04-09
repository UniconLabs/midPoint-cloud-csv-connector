package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import java.io.File;
import java.io.FilenameFilter;

/**
 * @author Viliam Repan (lazyman)
 */
public class SyncTokenFileFilter implements FilenameFilter {

    private String csvFileName;

    public SyncTokenFileFilter(String csvFileName) {
        this.csvFileName = csvFileName;
    }

    @Override
    public boolean accept(File parent, String fileName) {
        File file = new File(parent, fileName);
        if (file.isDirectory()) {
            return false;
        }

        if (fileName.matches(csvFileName.replaceAll("\\.", "\\\\.") + "\\.sync\\.[0-9]{13}$")) {
            return true;
        }

        return false;
    }
}
