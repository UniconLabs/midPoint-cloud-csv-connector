package com.evolveum.polygon.connector.cloud.objectstorage.csv.s3;

import io.netty.util.internal.StringUtil;
import org.identityconnectors.common.logging.Log;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.CloudCsvConfiguration;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.CloudStorageService;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Date;


public class AwsS3StorageService extends CloudStorageService {
    private static final Log LOG = Log.getLog(AwsS3StorageService.class);

    private S3Client s3Client;
    private Region region;

    public AwsS3StorageService() {
        this.region = Region.US_EAST_1;
        this.s3Client = S3Client.builder().region(region).build();
    }

    public AwsS3StorageService(final String region) {
        this.s3Client = S3Client.builder().region(Region.of(region)).build();
    }

    public AwsS3StorageService(final CloudCsvConfiguration config) throws UnknownHostException, URISyntaxException {
        final S3ClientBuilder builder = S3Client.builder();
        AwsS3ConfigurationBuilder.prepareClientBuilder(builder, config, config.isTestMode());
        this.s3Client = builder.build();
        this.region = (StringUtil.isNullOrEmpty(config.getRegion())) ? Region.US_EAST_1 : Region.of(config.getRegion());
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public void setS3Client(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public boolean checkBucketExists(final CloudCsvConfiguration config) throws Exception {
        return S3Utils.getObjectExists(config.getBucketName(), null, s3Client);
    }

    @Override
    public void createBucketIfNotExists(final CloudCsvConfiguration config) throws Exception {
        boolean exists = false;

        try {
            exists = S3Utils.getObjectExists(config.getBucketName(), null, s3Client);
        } catch (Exception e) {
            LOG.info("Error retrieving bucket {0} with {1}, attempting to creating bucket...", config.getBucketName(), e);
        }

        if (!exists) {
            S3Utils.createBucket(config.getBucketName(), region, s3Client);
        }
    }

    @Override
    public BufferedReader getFileAsReader(final CloudCsvConfiguration config) throws Exception {
        return S3Utils.getObjectAsReader(config.getBucketName(), config.getFileName(), s3Client);
    }

    @Override
    public void getFileAsAFile(final CloudCsvConfiguration config, final File fileToCopyTo) throws Exception {
        S3Utils.getObjectAsAFile(config.getBucketName(), config.getFileName(), fileToCopyTo, s3Client);
    }

    @Override
    public Date getFileLastUpdated(final CloudCsvConfiguration config) throws Exception {
        return S3Utils.getObjectLastUpdated(config.getBucketName(), config.getFileName(), s3Client);
    }

    @Override
    public boolean checkFileExistsAndCanRead(final CloudCsvConfiguration config) {
        boolean result = false;
        try {
            result = S3Utils.checkCanReadObject(config.getBucketName(), config.getFileName(), s3Client);
        } catch (Exception e) {
            LOG.info("Error attempting to check if file exists and connector can read for file {0} with error {1}", config.getFileName(), e);
        }

        return result;
    }

    @Override
    public void uploadFile(final CloudCsvConfiguration config, final File file) throws Exception {
        S3Utils.uploadFileToS3(config.getBucketName(), config.getFileName(), file, s3Client);
    }

    @Override
    public void uploadString(final CloudCsvConfiguration config, final String file) throws Exception {
        S3Utils.uploadStringToS3(config.getBucketName(), config.getFileName(), file, s3Client);
    }
}
