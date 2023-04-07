import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SQS_Service {

    private static SQS_Service instance = null;

    private final SqsClient sqsClient;

    private SQS_Service(){
        sqsClient = SqsClient.create();
    }

    public static SQS_Service getInstance(){
        if(instance == null){
            instance = new SQS_Service();
        }

        return instance;
    }

    /**
     * @param queueName
     * @return SQS queue URL
     */
    public String createQueue(String queueName) {
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

    public void sendMessage(String queueUrl, String message) {
        try {
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .delaySeconds(10)
                    .build();
            sqsClient.sendMessage(sendMsgRequest);
            addTagToQueue(queueUrl , "To" , "Yes");
        }
        catch (SqsException e) {
            System.err.println("ERROR: can't send the message!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public void addTagToQueue(String queueUrl , String s1 , String s2){
        try{
            TagQueueRequest tagQueueRequest = TagQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .tags(Map.of(s1 ,s2))
                    .build();
            sqsClient.tagQueue(tagQueueRequest);
        }
        catch (SqsException e){
            System.err.println("ERROR: can't tag the queue!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public List<Message> receiveMessage(String queueUrl){
        try{
            List<Message> listMessage = new ArrayList<>();
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .waitTimeSeconds(20)
                    .build();
            listMessage = sqsClient.receiveMessage(receiveMessageRequest).messages();

            return listMessage;
        }
        catch (SqsException e) {
            System.out.println("PRINT:MANAGER:SQS_service:receiveMessage - catch error");
            System.err.println("ERROR: a problem has occurred during reading messages from server.\n");
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    public ListQueuesResponse getListOfQueues(ListQueuesRequest listQueuesRequest){
        return sqsClient.listQueues(listQueuesRequest);
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
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }


}
