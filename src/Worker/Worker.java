import net.sourceforge.tess4j.Tesseract;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Worker {

    private final int workerNumber;
    private final String prefix;
    private final String prefix_workersToManager;
    private boolean terminate;
    private String managerToWorkers_queueURL;
    private String workersToManagerQueueURL;
    private boolean errorOccurred;
    private String error;
    private final String imagePathString;

    public Worker(int _workerNumber){
        workerNumber = _workerNumber;
        prefix = "managerToWorkers";
        prefix_workersToManager = "workersToManager";
        terminate = false;
        managerToWorkers_queueURL = "";
        workersToManagerQueueURL = "";
        errorOccurred = false;
        error = "";
        imagePathString = "imageToProcess.png";
    }

    //Get url of managerToWorker queue.
    private ListQueuesResponse getQueueResponse(String prefix){
        System.out.println("Worker: getQueueResponse");
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            return SQS_Service.getInstance().getQuesesResponse(listQueuesRequest);
        }
        catch (SqsException e) {
            System.err.println("ERROR: cannot get queue!\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return ListQueuesResponse.builder().build();
    }

    private String getQueueUrl(ListQueuesResponse ls){
        System.out.println("Worker: getQueueUrl");
        return ls.queueUrls().get(0);
    }

    private void processMessage(Message message){
        System.out.println("Worker: processMessage");
        String[] parsedMessage = parseMessage(message);
        String clientQueue = parsedMessage[0];
        String output;

//        if(parsedMessage.length != 2)
//        {
//            System.out.println("PRINT:WORKER:processMessage parsedMessage.length != 2  actually his length is =" + parsedMessage.length);
//            for(int i=0 ; i<parsedMessage.length ; i++){
//                System.out.println("PRINT:WORKER:processMessage  - parsedMessage.length != 2 and value in "  + i + " is = " + parsedMessage[i]);
//            }
//            output = makeOutput(clientQueue, "", "EmptyURl");
//            SQS_Service.getInstance().sendMessage(workersToManagerQueueURL, output);
//            SQS_Service.getInstance().deleteMessage(message, managerToWorkers_queueURL);
//            return;
//        }


        String imageURL = parsedMessage[1];
        String result = executeMessage(imageURL);

        if(!errorOccurred){
            output = makeOutput(clientQueue, imageURL, result);
        }
        else{
            output = makeOutput(clientQueue, imageURL, "Error is " + error);
        }
        SQS_Service.getInstance().sendMessage(workersToManagerQueueURL, output);
        SQS_Service.getInstance().deleteMessage(message, managerToWorkers_queueURL);
    }

    //Read max or all available messages
    private List<Message> readMessages(){
        System.out.println("Worker: readMessages");
        List<Message> message = SQS_Service.getInstance().receiveMessage(managerToWorkers_queueURL);
        return message;
    }

    //Message from manager is in the form of: queueUrl + "\n" + imageUrl
    private String[] parseMessage(Message message){
        System.out.println("Worker: parseMessage");
        return message.body().split("\\n");
    }

    private File downloadImage(String imageUrl){
        System.out.println("Worker: downloadImage");
        //download the image
        Path imagePath = Paths.get(imagePathString);
        try(InputStream inputStream = new URL(imageUrl).openStream()){
            Files.copy(inputStream, imagePath, StandardCopyOption.REPLACE_EXISTING);
        }
        catch(IOException e){
            errorOccurred = true;
            error = e.getMessage();
        }
        //open image and return
        return new File(imagePathString);
    }

    private String executeMessage(String imageUrl){
        System.out.println("Worker: executeOCR");
        File imageFile = downloadImage(imageUrl);
        Tesseract tesseract = new Tesseract();
        try{
            tesseract.setDatapath("Tess4J/tessdata");

            String text = tesseract.doOCR(imageFile);

            return text;
        }
        catch(net.sourceforge.tess4j.TesseractException e){

            errorOccurred = true;

            error = e.getMessage();

            System.exit(1);
        }
        return "";
    }

    private String executeMessage_noOCR(String imageUrl){
        System.out.println("Worker: executeMessage");
        File imageFile = downloadImage(imageUrl);
        String txt;
        try{
            txt = "executeOCR(imageFile) the imageFileUrl is : " + imageUrl;

        }catch (Exception e){
            txt = "executeOCR but in catch error";
        }

        return txt;
    }

    public void workerServe(){
        ListQueuesResponse queueList = getQueueResponse(prefix);
        managerToWorkers_queueURL = getQueueUrl(queueList);
        queueList = getQueueResponse(prefix_workersToManager);
        workersToManagerQueueURL = getQueueUrl(queueList);

        List<Message> messages;

        while(!terminate){
            messages = readMessages();

            if(messages.size() == 0){
                try{
                    TimeUnit.SECONDS.sleep(20);
                }
                catch(Exception e){
                    System.err.println("ERROR: worker cannot sleep!\n");
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
                messages = readMessages();
                if(messages.size() == 0){
                    terminate = true;
                }
            }
            else{
                processMessage(messages.get(0));
            }
        }
        onExit();
    }

    private void onExit(){
        SQS_Service.getInstance().onExit();
        changeTagToTerminate();
    }

    private String makeOutput(String clientQueue, String imageURl, String result){
        return clientQueue + "\n" + imageURl + "\n" + result;
    }

    private void changeTagToTerminate(){
        Ec2Client ec2 = Ec2Client.create();
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(19).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for(Tag tag : instance.tags()){
                            if(tag.key().equals(String.valueOf(workerNumber))){
                                //delete current tag
                                DeleteTagsRequest deleteTagsRequest = DeleteTagsRequest.builder()
                                        .resources(instance.instanceId())
                                        .tags(tag).build();
                                ec2.deleteTags(deleteTagsRequest);
                                //make new tag that signals manager to close current instance
                                Tag newTag = Tag.builder().key(String.valueOf(workerNumber)).value("Terminate").build();
                                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                                        .resources(instance.instanceId())
                                        .tags(newTag)
                                        .build();
                                ec2.createTags(tagRequest);
                                break;
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        }
        catch (Ec2Exception e) {
            System.err.println("ERROR: can't find instances in EC2\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        ec2.close();

    }

    private String executeOCR(File imageFile){
        Tesseract tesseract = new Tesseract();
        try{
            tesseract.setDatapath("Tess4J/tessdata");

            String text = tesseract.doOCR(imageFile);

            return text;
        }
        catch(net.sourceforge.tess4j.TesseractException e){

            errorOccurred = true;

            error = e.getMessage();

            return "";
        }

    }



}
