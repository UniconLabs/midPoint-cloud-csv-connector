package com.evolveum.polygon.connector.cloud.objectstorage.csv.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * CREATED BY UNICON
 */
public class S3Utils {


    private S3Utils() {
        throw new IllegalStateException("Utility class");
    }

    /** Retrun all the files in a folder
     * @param bucketName  The bucket
     * @param folderKey the folder
     * @return a list with all the keys.
     */

    public static List<String> getObjectslistFromFolder(String bucketName, String folderKey) {

        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(folderKey);

        List<String> keys = new ArrayList<>();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        for (;;) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.isEmpty()) {
                break;
            }
            summaries.forEach(s -> keys.add(s.getKey()));
            objects = s3Client.listNextBatchOfObjects(objects);
        }

        return keys;
    }

    public static S3ObjectInputStream getObjectAsInputStream(String bucketName, String key) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Obj = s3Client.getObject(bucketName, key);
        return s3Obj.getObjectContent();
    }

    public static byte[] getObjectAsByteArray(String bucketName, String key) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Obj = s3Client.getObject(bucketName, key);
        return IOUtils.toByteArray(s3Obj.getObjectContent());
    }

    /** Method to open the File from S3
     * @param bucketName The bucket where the file is
     * @param fileName the filename (including the path)
     * @return a BufferReader pointing to the file
     */
    public static BufferedReader openFile(String bucketName, String fileName, String encoding) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Obj = s3Client.getObject(bucketName, fileName);
        return new BufferedReader(new InputStreamReader(s3Obj.getObjectContent(), encoding));
    }

    public static void getInAFile(String bucketName, String fileName, File file) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3object = s3Client.getObject(bucketName, fileName);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        FileUtils.copyInputStreamToFile(inputStream, file);
    }


    public static BufferedReader openFile(String bucketName, String fileName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Obj = s3Client.getObject(bucketName, fileName);
        return new BufferedReader(new InputStreamReader(s3Obj.getObjectContent()));
    }

    public static BufferedReader openFileAgain(String bucketName, String fileName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Obj = s3Client.getObject(bucketName, fileName);
        return new BufferedReader(new InputStreamReader(s3Obj.getObjectContent()));
    }

    public static void copyObject(String fromBucket, String fromKey, String toBucket, String toKey) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.copyObject(new CopyObjectRequest(fromBucket, fromKey, toBucket, toKey));
    }

    public static void deleteObject(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
    }

    public static void moveObject(String fromBucket, String fromKey, String toBucket, String toKey) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.copyObject(new CopyObjectRequest(fromBucket, fromKey, toBucket, toKey));
        s3Client.deleteObject(new DeleteObjectRequest(fromBucket, fromKey));
    }

    public static String uploadFileToS3(String bucket, String directory, String filename, File file) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket,  directory + "/" + filename, file);
        s3Client.putObject(putObjectRequest);
        return directory + "/" + filename;
    }

    public static String uploadFileToS3(String bucket, String filename, File file) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, filename, file);
        s3Client.putObject(putObjectRequest);
        return filename;
    }

    /** Writes a S3 file from a String
     * @param string The string to write in the file
     * @param bucket The bucket
     * @param directory The folder where the file will be created
     * @param filename The file name
     * @param contentType The content type of the file
     * @return the key
     */
    public static String uploadStringToS3(String string, String bucket, String directory, String filename, String contentType) {
        byte[] fileContentBytes = string.getBytes(StandardCharsets.UTF_8);
        return uploadByteArrayToS3(fileContentBytes, bucket, directory, filename, contentType);
    }

    /** Writes a S3 file from a byte []
     * @param fileContentBytes The byte [] to write in the file
     * @param bucket The bucket
     * @param directory The folder where the file will be created
     * @param filename The file name
     * @param contentType The content type of the file
     * @return the key
     */
    public static String uploadByteArrayToS3(byte[] fileContentBytes, String bucket, String directory, String filename, String contentType) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        InputStream fileInputStream = new ByteArrayInputStream(fileContentBytes);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.setContentLength(fileContentBytes.length);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucket + "/" + directory, filename,
                fileInputStream, metadata);
        s3Client.putObject(putObjectRequest);
        return directory + "/" + filename;
    }

    public static Long getObjectSize(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        return s3Client.getObjectMetadata(bucket, key).getContentLength();
    }

    public static Date getObjectLastUpdated(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        return s3Client.getObjectMetadata(bucket, key).getLastModified();
    }

    public static Boolean getObjectExists(String bucket, String key) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        return s3Client.doesObjectExist(bucket, key);
    }

    public static void deleteFolderRecursively(String bucket, String path) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        List<String> toDelete = getObjectslistFromFolder(bucket, path);
        List<DeleteObjectsRequest.KeyVersion> bulk = new ArrayList<>();
        for (int i = 0; i < toDelete.size(); i++) {
            bulk.add(new DeleteObjectsRequest.KeyVersion(toDelete.get(i)));
            if (i % 100 == 0) {
                s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(bulk));
                bulk.clear();
            }
        }
        if (bulk.size() > 0) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(bulk));
        }
    }
}

