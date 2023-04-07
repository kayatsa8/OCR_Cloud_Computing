import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.*;
import java.util.List;

public class S3_Service {

    private static S3_Service instance = null;

    private final S3Client s3Client;

    private final String bucketName = "yourfriendlyneighborhoodbucketman";

    private String key;


    public static S3_Service getInstance(){
        if(instance == null){
            instance = new S3_Service();
        }

        return instance;
    }

    private S3_Service(){
        s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
        key = "";
    }

    public S3Client getClient(){
        return s3Client;
    }

    public boolean deleteFromS3(String bucketName , String keyName){
        try{
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            s3Client.deleteObject(deleteObjectRequest);
            return true;

        }catch(S3Exception e){
            System.err.println("ERROR: unable to delete file in bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return false;
        }
    }

    private boolean deleteAllFromBucket(String bucketName){
        try{
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();
            ListObjectsResponse res = s3Client.listObjects(listObjects);
            List<S3Object> objects = res.contents();
            for (S3Object myValue : objects) {
                boolean deleted = deleteFromS3(bucketName , myValue.key());
                if(!deleted)
                    return false;
            }
            return true;
        }catch (S3Exception e){
            System.err.println("ERROR: unable to delete file in bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return false;
        }
    }

    public boolean deleteBucket(String bucketName){
        try{
            if(isBucketExists(bucketName)){
                boolean allDeleted = deleteAllFromBucket(bucketName);
                if(!allDeleted)
                    return false;
                DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                        .bucket(bucketName)
                        .build();
                s3Client.deleteBucket(deleteBucketRequest);
                return true;
            }
            return false;
        }catch (S3Exception e){
            System.err.println("ERROR: unable to delete bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return false;

        }
    }

    public void uploadInputToS3(String fileName, String keyName){
        File file = getFile(fileName);

        if(!isBucketExists(bucketName)){
            createBucket();
        }
        putFileToS3(file);
    }

    private File getFile(String inputName){
        File file = new File(inputName);

        if(!file.exists() || file.isDirectory()) {
            System.err.println("ERROR: the given file was not found!\n");
        }

        return file;
    }

    private boolean isBucketExists(String bucketName){
        ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets(listBucketsRequest);

        for(Bucket bucket : listBucketsResponse.buckets()){
            if(bucket.name().equals(bucketName)){
                return true;
            }
        }

        return false;
    }

    private void createBucket(){
        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName +" is ready");

        }
        catch (S3Exception e) {
            System.err.println("Can't create the requested bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }

    public void makeKey(String keyName){
        key = keyName;
    }

    private void putFileToS3(File file){
        try{
            s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(), RequestBody.fromFile(file));
        }
        catch(S3Exception e){
            System.err.println("ERROR: unable to put file in bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public String getBucketName(){
        return bucketName;
    }

    public String getKey(){
        return key;
    }

    public String fileLocationAtS3(String numberOfUrlsForWorker){
        return MessageMaker.makeLocation(bucketName, key, numberOfUrlsForWorker);
    }

    public File downloadFromS3(String location){

        String answerBucket = MessageMaker.getBucket(location);
        String answerKey = MessageMaker.getKey(location);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(answerBucket)
                .key(answerKey)
                .build();

        ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(getObjectRequest);

        try{
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(answerKey));
            byte[] buffer = new byte[4096];
            int bytesRead = -1;

            while((bytesRead = inputStream.read(buffer)) != -1){
                outputStream.write(buffer, 0, bytesRead);
            }

            inputStream.close();;
            outputStream.close();

        }
        catch (FileNotFoundException e){
            System.err.println("ERROR: FileOutputStream\n");
            System.exit(1);
        }
        catch (IOException e) {
            System.err.println("ERROR: can't read from server");
            System.exit(1);
        }

        return new File(answerKey);

    }

    public String fileLocationAtS3_terminate(String numberOfUrlsForWorker){
        return MessageMaker.makeLocation_terminate(bucketName, key, numberOfUrlsForWorker);
    }

    public void closeClient(){
        s3Client.close();
    }




}
