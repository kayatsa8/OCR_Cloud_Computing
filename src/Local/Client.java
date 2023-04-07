import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Client {
    private String username;
    private final String inputFileName;
    private final String outputFileName;
    private final Ec2Client ec2;
    private boolean askTerminate;
    private String queueUrl;
    private final String numberOfUrlsForWorker;
    private boolean clientNeedToDeletePersonalQueue;


    public Client(String _inputFileName, String _outputFileName, String _numberOfUrlsForWorker){
        username = "";
        inputFileName = _inputFileName;
        outputFileName = _outputFileName;
        ec2 = Ec2Client.builder().credentialsProvider(ProfileCredentialsProvider.create()).build();
        askTerminate = false;
        queueUrl = "";
        numberOfUrlsForWorker = _numberOfUrlsForWorker;
        clientNeedToDeletePersonalQueue = true;
    }

    public void askForTermination(String terminate){
        askTerminate = terminate.equals("terminate");
    }

    public void run(){
        System.out.println("PRINT-CLIENT:run");

        getUsername();
        uploadFile(inputFileName, username);
        getManager();
        notifyManager();
        File file = getAnswer(queueUrl); //waiting until tag "To" of queue is "Yes" (Map<String , String>).
        createHTML(file);
        onExit();

    }

    private void getUsername(){
        System.out.println("Enter your nickname please\n");
        Scanner s1 = new Scanner(System.in);
        username = s1.nextLine();
    }

    private void uploadFile(String fileName , String keyName){
        try{
            S3_Service.getInstance().makeKey(keyName + ".txt");
            S3_Service.getInstance().uploadInputToS3(fileName , keyName);
        }
        catch(Exception e){
            System.err.println("ERROR: can't upload file to server!\n");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private Instance getManager(){
        Instance instance = checkManagerOn();

        if(instance == null){
            instance = createManager();
        }

        return instance;
    }

    private Instance checkManagerOn(){
        //ASSUMPTION: tag.key = "Name" tag.value = "manager"
        boolean done = false;
        String nextToken = null;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(19).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for(Tag tag : instance.tags()){
                            if(tag.key().equals("Name") &&
                                    tag.value().equals("manager")
                                    && !instance.state().nameAsString().equals("terminated")){
                                done = true;
                                return instance;
                            }
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null && !done);
        }
        catch (Ec2Exception e) {
            System.err.println("ERROR: can't find instances in EC2\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return null; // null = manager was not found
    }

    private Instance createManager() {
        String userData = "#!/bin/bash\n";

        userData += "yum update -y\n";
        userData += "amazon-linux-extras install java-openjdk11 -y\n";
        userData += "java -version\n";
        userData += "pwd\n";
        userData += "aws s3 cp s3://yourfriendlyneighborhoodbucketman/Manager.jar downloadManager.jar\n";
        userData += "ls -l\n";
        userData += "sudo java -jar downloadManager.jar\n";


        S3_Service.getInstance().makeKey("Manager.jar");
        S3_Service.getInstance().uploadInputToS3("Manager.jar","Manager.jar");
        S3_Service.getInstance().makeKey("Worker.jar");
        S3_Service.getInstance().uploadInputToS3("Worker.jar","Worker.jar");
        S3_Service.getInstance().makeKey("tessdata.zip");
        S3_Service.getInstance().uploadInputToS3("tessdata.zip", "tessdata.zip");
        S3_Service.getInstance().makeKey(username + ".txt");

        String base64UserData =  null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }


        String amiId = "ami-0b0dcb5067f052a63";
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_SMALL)
                .maxCount(1)
                .imageId(amiId)
                .minCount(1)
                .iamInstanceProfile(software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .instanceInitiatedShutdownBehavior("terminate")
                .keyName("vockey")
                .userData(base64UserData)
                .build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();
        Instance instance = instances.get(0);
        tagInstanceAsManager(instance);

        return instance;
    }

    private void tagInstanceAsManager(Instance instance){
        Tag tag = Tag.builder().key("Name").value("manager").build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instance.instanceId())
                .tags(tag)
                .build();
        ec2.createTags(tagRequest);
    }

    private void notifyManager(){
        String message;

        if(!askTerminate){
            message = S3_Service.getInstance().fileLocationAtS3(numberOfUrlsForWorker);
        }
        else{
            message = S3_Service.getInstance().fileLocationAtS3_terminate(numberOfUrlsForWorker);
        }
        queueUrl = SQS_Service.getInstance().createQueue(username);
        SQS_Service.getInstance().sendMessage(queueUrl , message);

    }

    private File getAnswer(String queueUrl){
        /*
         * 1) busy wait to manager - on sqs
         * 2) retrieve answer in message / on sqs
         */
        /*constant check for message sent to client's queue. while sent , queue value of key "To" is changed to "Yes"*/
        String value = "";
        do{
            value = SQS_Service.getInstance().getTagOfQueue(queueUrl , "To");
        }while(value == null || !value.equals("Yes"));

        List<Message> listMessage = SQS_Service.getInstance().receiveMessage(queueUrl);
        if(!listMessage.isEmpty()){
            Message message = listMessage.get(0);
            String location = message.body();
            File toReturn = S3_Service.getInstance().downloadFromS3(location);
            SQS_Service.getInstance().deleteMessage(message, queueUrl);
            S3_Service.getInstance().deleteFromS3(MessageMaker.getBucket(location) , MessageMaker.getKey(location));
            terminateManagerIfRequired(location);
            System.out.println(location);
            return toReturn;
        }
        return null;
    }

    private void createHTML(File file){
        String html_start = "<!-- saved from url=(0048)https://s3.amazonaws.com/dsp132/text.images.html -->\n" +
                "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1252\"><title>OCR</title>\n" +
                "</head><body>\n" +
                "\t<p>\n";
        String imgSrc_start = "\t\t<img src=\"";
        String imgSrc_end = "\"><br>\n";
        String text_start = "\t\t";
        String text_end = "\n";
        String between = "\t</p>\n" + "\t<p>\n";
        String html_end = "</body></html>";

        StringBuilder html = new StringBuilder();

        List<String> inputFile = file2List(file);

        html.append(html_start);

        for(int i=0; i<inputFile.size(); i+=2){
            html.append(imgSrc_start);
            html.append(inputFile.get(i));
            html.append(imgSrc_end);
            html.append(text_start);
            html.append(inputFile.get(i+1));
            html.append(text_end);

            if(i+1<inputFile.size()){
                html.append(between);
            }
        }

        html.append(html_end);

        File outputFile = new File(outputFileName);

        try{
            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
            bw.write(html.toString());
            bw.close();
        }
        catch(IOException e){
            System.err.println("Error:\n");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void onExit(){
        if(clientNeedToDeletePersonalQueue)
            SQS_Service.getInstance().deleteQueue(queueUrl);
        S3_Service.getInstance().closeClient();
        SQS_Service.getInstance().closeClient();
    }

    private void terminateManagerIfRequired(String location){
        if(MessageMaker.shouldTerminateManager(location)){
           try{
               // for safe termination, local is waiting for manager to stop and then terminates it
               TimeUnit.SECONDS.sleep(30);

               boolean done = false;
               String nextToken = null;
               try {
                   do {
                       DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(19).nextToken(nextToken).build();
                       DescribeInstancesResponse response = ec2.describeInstances(request);
                       for (Reservation reservation : response.reservations()) {
                           for (Instance instance : reservation.instances()) {
                               for(Tag tag : instance.tags()){
                                   if(tag.key().equals("Name") && tag.value().equals("manager")){
                                       terminateInstance(ec2, instance.instanceId() , location);
                                       clientNeedToDeletePersonalQueue = false;
                                   }
                                   else{
                                       System.err.println("An instance is still working!!!\n");
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


           }
           catch (Exception e){
               System.err.println("ERROR: client can't sleep\n");
               System.err.println(e.getMessage());
               System.exit(1);
           }
        }

    }

    private void terminateInstance(Ec2Client ec2, String instanceId , String location) {
        try{
            S3_Service.getInstance().deleteBucket(MessageMaker.getBucket(location));
            SQS_Service.getInstance().deleteAllQueues();

            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public void setUsername(String _username){
        username = _username;
    }

    private List<String> file2List(File file){
        List<String> list = new ArrayList<>();

        try {
            Scanner reader = new Scanner(file);
            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                list.add(line);
            }
            reader.close();
        }
        catch (FileNotFoundException e) {
            System.err.println("An error occurred.");
            e.printStackTrace();
            System.exit(1);
        }

        return list;
    }

}


