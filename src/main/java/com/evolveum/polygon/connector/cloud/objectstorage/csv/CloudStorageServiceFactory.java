package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.s3.AwsS3StorageService;
import java.util.Arrays;

public class CloudStorageServiceFactory {

    public static CloudStorageService getCloudServiceProvider(final CloudCsvConfiguration config) throws Exception {
        final CloudObjectProviders provider = Arrays.stream(CloudObjectProviders.values())
                .filter(p -> p.toString().equalsIgnoreCase(config.getCloudObjectStorageProvider()))
                .findFirst().orElse(null);

        if (provider != null && provider.name().equalsIgnoreCase("s3")) {
            return new AwsS3StorageService(config);

        } else if (provider != null && provider.name().equalsIgnoreCase("blob")) {
            //TODO Replace Below With Others Here
            return new AwsS3StorageService(config);

        } else {
            return new AwsS3StorageService(config); //TODO Default to S3??
        }
    }
}
