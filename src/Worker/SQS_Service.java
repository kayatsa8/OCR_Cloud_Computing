import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.List;

public class SQS_Service {
    /**
     * 1.create queue
     * 2.send message
     * 3.busy-wait for manager response and return S3's location.
     *
     * */

    private static SQS_Service instance = null;
    private final SqsClient sqsClient;


    private SQS_Service(){
        this.sqsClient =SqsClient.create();
    }

    public static SQS_Service getInstance(){

        if(instance == null){
            instance = new SQS_Service();
        }

        return instance;

    }

    public ListQueuesResponse getQuesesResponse(ListQueuesRequest listQueuesRequest){
        return this.sqsClient.listQueues(listQueuesRequest);
    }

    public void sendMessage(String queueUrl, String message) {
        try {
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .delaySeconds(10)
                    .build();
            sqsClient.sendMessage(sendMsgRequest);
        }
        catch (SqsException e) {
            System.err.println("ERROR: can't send the message to manager!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public List<Message> receiveMessage(String queueUrl){
        int noMoreWait = 2;
        try{
            List<Message> messageList = new ArrayList<>();
            while (messageList.isEmpty() && noMoreWait >0) {
                ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(10)
                        .build();
                messageList = sqsClient.receiveMessage(receiveMessageRequest).messages();
                if(messageList.isEmpty())
                    noMoreWait--;
                else
                    noMoreWait = 2;
            }
            return messageList;
        }
        catch (SqsException e) {
            System.err.println("ERROR: can't receive message from manager!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public void deleteMessage(Message message, String queueUrl){
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl).receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public void onExit(){
        System.out.println("SQS_Service: onExit");
        sqsClient.close();
    }






}
