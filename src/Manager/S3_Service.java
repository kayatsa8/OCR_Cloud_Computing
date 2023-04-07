import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.*;

public class S3_Service {

    private static S3_Service instance = null;

    private final S3Client s3Client;


    public static S3_Service getInstance(){
        if(instance == null){
            instance = new S3_Service();
        }

        return instance;
    }

    private S3_Service(){
        s3Client = S3Client.create();
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
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
            return false;
        }
    }

    public void uploadInputToS3(File file, String location){

        if(!isBucketExists(MessageMaker.getBucket(location))){
            createBucket(MessageMaker.getBucket(location));
        }

        String answerLocation = MessageMaker.makeLocation(location);

        putFileToS3(file, answerLocation);

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

    private void createBucket(String bucketName){

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
        }
        catch (S3Exception e) {
            System.err.println("Can't create the requested bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

    }

    private void putFileToS3(File file, String location){
        try{
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(MessageMaker.getBucket(location))
                    .key(MessageMaker.getKey(location)).build(),
                    RequestBody.fromFile(file));
        }
        catch(S3Exception e){
            System.err.println("ERROR: unable to put file in bucket!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public File downloadFromS3(String location){

        String answerBucket = MessageMaker.getBucket(location);
        String answerKey = MessageMaker.getKey(location);

        ResponseInputStream<GetObjectResponse> inputStream = null;

        try{
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(answerBucket)
                    .key(answerKey)
                    .build();

            inputStream = s3Client.getObject(getObjectRequest);
        }
        catch(Exception e){
            System.err.println("ERROR: can't get the requested object from S3!");
            System.err.println(e.getMessage());
            System.err.println("Searched location is: " + location);
            System.err.println("Bucket: " + answerBucket);
            System.err.println("Key: " + answerKey);
        }



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
        }
        catch (IOException e) {
            System.err.println("ERROR: can't read from server");
        }

        return new File(answerKey);

    }

    public void closeClient(){
        s3Client.close();
    }

}
