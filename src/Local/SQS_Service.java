import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQS_Service {

    private static SQS_Service instance = null;

    private final SqsClient sqsClient;

    private SQS_Service(){
        sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    public static SQS_Service getInstance(){
        if(instance == null){
            instance = new SQS_Service();
        }

        return instance;
    }

    /**
     * @param username
     * @return SQS queue URL
     */
    public String createQueue(String username) {
        String queueName = "Client" + username;
        try {
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            sqsClient.createQueue(createQueueRequest);
            GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            return getQueueUrlResponse.queueUrl();
        }
        catch (SqsException e) {
            System.err.println("ERROR: can't create the queue!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public boolean deleteAllQueues(){
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix("").build();
            ListQueuesResponse ls = sqsClient.listQueues(listQueuesRequest);
            for( String queueUrl : ls.queueUrls())
                deleteQueue(queueUrl);
            return true;
        }
        catch (SqsException e) {
            System.err.println("ERROR: problem in searching queues");
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }

    public String getTagOfQueue(String queueUrl , String s1){
        try{
            ListQueueTagsRequest listQueueTagsRequest = ListQueueTagsRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            Map<String,String> tags = sqsClient.listQueueTags(listQueueTagsRequest).tags();
            return tags.get(s1);
        }
        catch (SqsException e){
            System.err.println("ERROR: can't tag the queue!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void sendMessage(String queueUrl, String message) {
        try {
            System.out.println("PRINT:LOCAL:SQS_service:sendMessage - at start + " + message);
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .delaySeconds(10)
                    .build();
            sqsClient.sendMessage(sendMsgRequest);
            System.out.println("PRINT:LOCAL:SQS_service:after sendMessage - at start + " + message);
        }
        catch (SqsException e) {
            System.out.println("PRINT:LOCAL:SQS_service:sendMessage - catch error");
            System.err.println("ERROR: can't send the message!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public List<Message> receiveMessage(String queueUrl){
        try{
            List<Message> listMessage = new ArrayList<>();
            while (listMessage.isEmpty()) {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(20)
                        .build();
                listMessage = sqsClient.receiveMessage(receiveMessageRequest).messages();
            }
            return listMessage;
        }
        catch (SqsException e) {
            System.err.println("ERROR: a problem has occurred during reading messages from server.\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void closeClient(){
        sqsClient.close();
    }

    public void deleteQueue(String queueURL){
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueURL)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);
        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void deleteMessage(Message message, String queueUrl){
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl).receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public ListQueuesResponse getListOfQueues(ListQueuesRequest listQueuesRequest){
        return sqsClient.listQueues(listQueuesRequest);
    }

    private ListQueuesResponse getAllQueuesResponse(String prefix){
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            return getListOfQueues(listQueuesRequest);
        }
        catch (SqsException e) {
            System.err.println("ERROR: problem in searching queues");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return ListQueuesResponse.builder().build();
    }


}
