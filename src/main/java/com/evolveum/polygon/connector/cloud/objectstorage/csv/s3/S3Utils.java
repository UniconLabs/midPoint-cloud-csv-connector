package com.evolveum.polygon.connector.cloud.objectstorage.csv.s3;

import org.identityconnectors.common.logging.Log;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.utils.StringUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


/**
 * CREATED BY UNICON
 */
public class S3Utils {

    private static final Log LOG = Log.getLog(S3Utils.class);
    private static final String separator = "/";

    private S3Utils() {
        throw new IllegalStateException("Utility class");
    }

    /** Retrun all the files in a folder
     * @param bucketName  The bucket
     * @return a list with all the keys.
     */

    public static String createBucket(String bucketName, Region region, S3Client s3Client) throws Exception {
        final S3Waiter s3Waiter = s3Client.waiter();
        CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .locationConstraint(region.id())
                                .build())
                .build();

        s3Client.createBucket(bucketRequest);
        HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

        // Wait until the bucket is created and print out the response
        WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
        waiterResponse.matched().response().ifPresent(result -> LOG.info(result.toString()));
        LOG.info("Created " + bucketName + " and it's ready!");

        return bucketName;
    }

    public static List<String> getObjectslistFromFolder(String bucketName, String folderKey, S3Client s3Client) throws Exception {
        final ListObjectsResponse response = s3Client.listObjects(ListObjectsRequest.builder().bucket(bucketName).prefix(folderKey).build());

        return response.contents().stream()
                .filter(obj -> StringUtils.isNotBlank(obj.key()))
                .map(obj -> obj.key())
                .collect(Collectors.toList());
    }

    /** Method to open the File from S3
     * @param bucketName The bucket where the file is
     * @param fileName the filename (including the path)
     * @return a BufferReader pointing to the file
     */
    public static BufferedReader openFile(String bucketName, String fileName, String encoding, S3Client s3Client) throws Exception {
        final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileName).build());

        return new BufferedReader(new InputStreamReader(response, encoding));
    }

    public static void getObjectAsAFile(String bucketName, String fileName, File file, S3Client s3Client) throws Exception {
        final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileName).build());
        FileUtils.copyInputStreamToFile(response, file);
    }


    public static BufferedReader openFile(String bucketName, String fileName, S3Client s3Client) throws Exception {
        final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileName).build());

        return new BufferedReader(new InputStreamReader(response));
    }

    public static BufferedReader openFileAgain(String bucketName, String fileName, S3Client s3Client) throws Exception {
        final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileName).build());

        return new BufferedReader(new InputStreamReader(response));
    }

    public static void copyObject(String fromBucket, String fromKey, String toBucket, String toKey, S3Client s3Client) throws Exception {
        s3Client.copyObject(CopyObjectRequest.builder()
                .copySource(fromBucket + separator + fromKey)
                .destinationBucket(toBucket)
                .destinationKey(toKey)
                .build());
    }

    public static void deleteObject(String bucket, String key, S3Client s3Client) throws Exception {
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    public static void moveObject(String fromBucket, String fromKey, String toBucket, String toKey, S3Client s3Client) throws Exception {
        s3Client.copyObject(CopyObjectRequest.builder()
                .copySource(fromBucket + separator + fromKey)
                .destinationBucket(toBucket)
                .destinationKey(toKey)
                .build());
        s3Client.deleteObject(DeleteObjectRequest.builder()
                .bucket(fromBucket)
                .key(fromKey)
                .build());
    }

    public static String uploadFileToS3(String bucket, String directory, String filename, File file, S3Client s3Client) throws Exception {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(directory + separator + filename)
                .build(),
                RequestBody.fromFile(file));

        return directory + separator + filename;
    }

    public static String uploadFileToS3(String bucket, String filename, File file, S3Client s3Client) throws Exception {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(filename)
                        .build(),
                RequestBody.fromFile(file));

        return filename;
    }

    public static boolean checkCanReadObject(String bucketName, String fileName, S3Client s3Client) throws Exception {
        if (!S3Utils.getObjectExists(bucketName, fileName, s3Client)) {
            LOG.error("BucketName or FileName are not correct");
            return false;
        }

        return true;
    }

    public static BufferedReader getObjectAsReader(String bucketName, String fileName, S3Client s3Client) throws Exception {
        return S3Utils.openFile(bucketName, fileName, s3Client);
    }

    /** Writes a S3 file from a String
     * @param string The string to write in the file
     * @param bucket The bucket
     * @param directory The folder where the file will be created
     * @param filename The file name
     * @param contentType The content type of the file
     * @return the key
     */
    public static String uploadStringToS3(String string, String bucket, String directory, String filename, String contentType, S3Client s3Client) throws Exception {
        byte[] fileContentBytes = string.getBytes(StandardCharsets.UTF_8);
        return uploadByteArrayToS3(fileContentBytes, bucket, directory, filename, contentType, s3Client);
    }

    /** Writes a S3 file from a String
     * @param stringToUpload The string to write in the file
     * @param bucket The bucket
     * @param filename The file name
     * @return the key
     */
    public static String uploadStringToS3(String bucket, String filename, String stringToUpload, S3Client s3Client) throws Exception {
        byte[] fileContentBytes = stringToUpload.getBytes(StandardCharsets.UTF_8);
        return uploadByteArrayToS3(fileContentBytes, bucket, filename, s3Client);
    }

    /** Writes a S3 file from a byte []
     * @param fileContentBytes The byte [] to write in the file
     * @param bucket The bucket
     * @param directory The folder where the file will be created
     * @param filename The file name
     * @param contentType The content type of the file
     * @return the key
     */
    public static String uploadByteArrayToS3(byte[] fileContentBytes, String bucket, String directory, String filename, String contentType, S3Client s3Client) throws Exception {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket + separator + directory)
                        .key(filename)
                        .build(),
                RequestBody.fromBytes(fileContentBytes));

        s3Client.putObject(PutObjectRequest.builder().bucket(bucket + separator + directory).key(filename).build(), RequestBody.fromBytes(fileContentBytes));
        return directory + separator + filename;
    }

    /** Writes a S3 file from a byte []
     * @param fileContentBytes The byte [] to write in the file
     * @param bucket The bucket
     * @param filename The file name
     * @return the key
     */
    public static String uploadByteArrayToS3(byte[] fileContentBytes, String bucket, String filename, S3Client s3Client) throws Exception {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(filename)
                        .build(),
                RequestBody.fromBytes(fileContentBytes));

        s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(filename).build(), RequestBody.fromBytes(fileContentBytes));
        return filename;
    }

    public static Long getObjectSize(String bucket, String key, S3Client s3Client) throws Exception {
        return s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).contentLength();
    }

    public static Date getObjectLastUpdated(String bucket, String key, S3Client s3Client) throws Exception {
        return Date.from(s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()).lastModified());
    }

    public static Boolean getObjectExists(String bucket, String key, S3Client s3Client) throws Exception  {
        final  S3Response response;

        if (key == null) {
            response = s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } else {
            response = s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
        }

        if (response != null && response.sdkHttpResponse().statusCode() == 200) {
            return true;
        }

        return false;
    }

    public static void deleteFolderRecursively(String bucket, String path, S3Client s3Client) throws Exception {
        final ListObjectsResponse response = s3Client
                .listObjects(ListObjectsRequest
                        .builder()
                        .bucket(bucket)
                        .prefix(path)
                        .build());

        final List<ObjectIdentifier> toDelete = response.contents().stream()
                .map(key -> ObjectIdentifier.builder().key(key.key()).build())
                .collect(Collectors.toList());

        s3Client.deleteObjects(DeleteObjectsRequest.builder()
                .bucket(bucket).delete(Delete.builder().objects(toDelete).build()).build());
    }
}

