package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionImpl;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.S3Utils;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

public class CloudStorageService {


    AmazonS3 s3Client;

    public AmazonS3 getS3Client() {
        return s3Client;
    }

    public void setS3Client(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public CloudStorageService() {
        this.s3Client = AmazonS3ClientBuilder.standard().build();
    }

    public CloudStorageService(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    public CloudStorageService(String region) {
        this.s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    }

    public BufferedReader openFile(String bucketName, String fileName, String encoding) throws IOException {
        return S3Utils.openFile(bucketName, fileName, encoding, s3Client);
    }

    public void getInAFile(String bucketName, String fileName, File file) throws IOException {
        S3Utils.getInAFile(bucketName, fileName, file, s3Client);
    }

    public BufferedReader openFile(String bucketName, String fileName) {
        return S3Utils.openFile(bucketName, fileName, s3Client);
    }

    public String uploadFileToS3(String bucket, String directory, String filename, File file) throws IOException {
        return S3Utils.uploadFileToS3(bucket, directory, filename, file, s3Client);
    }

    public String uploadFileToS3(String bucket, String filename, File file) throws IOException {
        return S3Utils.uploadFileToS3(bucket, filename, file, s3Client);
    }

    public Date getObjectLastUpdated(String bucket, String key) throws IOException {
        return S3Utils.getObjectLastUpdated(bucket, key, s3Client);
    }

    public Boolean getObjectExists(String bucket, String key) throws IOException {
        return S3Utils.getObjectExists(bucket, key, s3Client);
    }

    public Boolean getObjectExists(String bucket) throws IOException {
        return S3Utils.getObjectExists(bucket, null, s3Client);
    }

    public BufferedReader createReaderS3(ObjectClassHandlerConfiguration configuration) throws IOException {
        return Util.createReaderS3(configuration, s3Client);
    }

    public BufferedReader createReaderS3(String fileName, ObjectClassHandlerConfiguration configuration) throws IOException {
        return Util.createReaderS3(fileName, configuration, s3Client);
    }

    public void checkCanReadFileS3(String bucketName, String fileName) {
        Util.checkCanReadFileS3(bucketName, fileName, s3Client);
    }

    public BufferedReader createReader(CsvConfiguration configuration) throws IOException {
        return Util.createReader(configuration, s3Client);
    }

    public BufferedReader createReaderS3(String fileName, CsvConfiguration configuration) throws IOException {
        return Util.createReaderS3(fileName, configuration, s3Client);
    }

    public List<String> getObjectslistFromFolder(String bucketName, String folderKey) {
        return S3Utils.getObjectslistFromFolder(bucketName,folderKey,s3Client);
    }



}
