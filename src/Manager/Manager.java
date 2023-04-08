import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class Manager {

    private final ActorThreadPool mainPool;
    private final ActorThreadPool pool;
    private final ConcurrentLinkedQueue<String> sqsUrlsClients = new ConcurrentLinkedQueue<>(); //List of queueUrl of clients.
    private final ConcurrentHashMap<String, ImageTasks> tasks; //Hash map - give queueUrl get corresponding BigObject
    private boolean terminate; //True if client sent termination sign to manager via cmd (while creating the manager)
    private Boolean shouldTerminate; //useful boolean to handle termination while old tasks need to be done before.
    private final SQS_Service sqs_service; //Singleton SQS_Service
    private final S3_Service s3_service; //Singleton S3_Service
    private final String managerToWorkersQueueURL;
    private final String workersToManagerQueueURL;
    private final int maxWorkers;
    private int currentWorkers;
    private final boolean[] activeWorkerNumbers;
    static int thread1servecounter = 0;


    public Manager(int threadsNumber){
        mainPool = new ActorThreadPool(2);
        pool = new ActorThreadPool(threadsNumber);
        tasks = new ConcurrentHashMap<>();
        terminate = false;
        shouldTerminate = false;
        sqs_service = SQS_Service.getInstance();
        s3_service  = S3_Service.getInstance();
        managerToWorkersQueueURL = sqs_service.createQueue("managerToWorkers");
        workersToManagerQueueURL = sqs_service.createQueue("workersToManager");
        maxWorkers = 18;
        currentWorkers = 0;
        activeWorkerNumbers = new boolean[maxWorkers];
    }

    public void run(){
        mainPool.submit(thread1Serve());
        mainPool.submit(thread2Serve());
    }

    //Both thread1 and thread2 used-functions.
    //get all queues from nextToken with name starts by given prefix.
    private ListQueuesResponse getAllQueuesResponse(String prefix){
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
            return sqs_service.getListOfQueues(listQueuesRequest);
        }
        catch (SqsException e) {
            System.err.println("ERROR: problem in searching queues");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return ListQueuesResponse.builder().build();
    }

    //get all urls of queues whom we have not read message from yet
    private LinkedList<String> urlNeedToServeQueues(ListQueuesResponse ls){
        //The function returns at most 1000 queues of not yet serviced queues,
        // therefore we should take care of nextToken.
        LinkedList<String> queuesToServe = new LinkedList<>();
        for (String queueUrl : ls.queueUrls()){
            if(!sqsUrlsClients.contains(queueUrl)){
                queuesToServe.add(queueUrl);
            }

        }
        return queuesToServe;
    }

    private void cleanUrlsNoLongerToServe(ListQueuesResponse ls){
        List<String> toRemove = new LinkedList<>();

        for(String url : sqsUrlsClients){
            if(!ls.queueUrls().contains(url)){
                toRemove.add(url);
                System.out.println("URL to remove: " + url);
            }
        }
        for(String url : toRemove){
            sqsUrlsClients.remove(url);
        }

    }

    //Read message from queue with the specific given input queueUrl.
    private Message readMessage(String queueUrl){
        List<Message> message = sqs_service.receiveMessage(queueUrl);

        if(message.isEmpty()){
            return null;
        }
        else{
            return message.get(0);
        }
    }

    //Make runnable object from given message. We want to attach origin queueUrl (in order to know sender's queue),
    //mode is either 1 or 2  - accordingly each of the two different main threads need to execute different runnable task.
    //This is the part of what thread from the pool needs to do.

    private Runnable makeClientMessageRunnable(String queueUrl, Message message){
        return () -> {
            //Download file from s3 , location needs to be decoded from message.
            File file = s3_service.downloadFromS3(message.body());
            //Create ImageTasks for client (=File)
            ImageTasks imageTasks = new ImageTasks(file, queueUrl, message.body(), message);
            //Put ImageTasks in list has 2 main goals: 1) tell thread1 to not terminate if the list is not empty
            //2) use each ImageTasks as collection of urls images and text extracted for each image.
            tasks.put(queueUrl, imageTasks);
            //Send message to workers that they need to process new image.
            createNewWorkers(message.body(), imageTasks);
            for(String imageUrl : imageTasks.getUrlText().keySet()){
                sqs_service.sendMessage(managerToWorkersQueueURL, queueUrl + "\n" + imageUrl);
            }

            sqsUrlsClients.add(queueUrl);

            if(MessageMaker.shouldTerminate(message.body())){
                synchronized (shouldTerminate){
                    shouldTerminate = true;
                }
            }

            createNewWorkers(message.body(), imageTasks);
        };
    }

    private void createNewWorkers(String message, ImageTasks imageTasks){
        int newMissions = imageTasks.getUrlText().keySet().size();
        int urlsForWorker = MessageMaker.getNumberOfUrlsForWorker(message);
        int numberOfNeededWorkers = newMissions / urlsForWorker;

        if(numberOfNeededWorkers == 0 && newMissions > 0){
            numberOfNeededWorkers = 1;
        }

        int numberOfNewWorkers = numberOfNeededWorkers - currentWorkers;
        int toCreate = 0;



        if(numberOfNewWorkers > 0 && currentWorkers < maxWorkers){
            if(numberOfNewWorkers > maxWorkers-currentWorkers){
                toCreate = maxWorkers - currentWorkers;
            }
            else{
                toCreate = numberOfNewWorkers;
            }

            Ec2Client ec2 = Ec2Client.create();
            List<Instance> instances = new LinkedList<>();

            for(int i=0; i<toCreate; i++){
                instances.addAll(createSingleWorker(ec2));
            }

            tagInstancesAsWorkers(instances);

            currentWorkers += toCreate;
            ec2.close();

        }

    }

    private List<Instance> createSingleWorker(Ec2Client ec2){
        int workerNumber = getNotActiveWorkerNumber();
        String amiId = "ami-0b0dcb5067f052a63";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T3_SMALL)
                .maxCount(1)
                .minCount(1)
                .imageId(amiId)
                .iamInstanceProfile(software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .keyName("vockey")
                .instanceInitiatedShutdownBehavior("terminate")
                .userData(Base64.getEncoder().encodeToString(
                        makeBashForWorker(workerNumber).getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        List<Instance> instances = response.instances();
        Instance instance = instances.get(0);

        Tag tag = Tag.builder().key(String.valueOf(workerNumber)).value("working").build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instance.instanceId())
                .tags(tag)
                .build();
        ec2.createTags(tagRequest);

        activeWorkerNumbers[workerNumber] = true;

        return instances;
    }

    private String makeBashForWorker(int workerNumber){

        String userData = "#!/bin/bash\n";
        userData += "yum update -y\n";
        userData += "amazon-linux-extras install java-openjdk11 -y\n";
        userData += "java -version\n";
        userData += "pwd\n";
        userData += "aws s3 cp s3://yourfriendlyneighborhoodbucketman/Worker.jar downloadWorker.jar\n";
        userData += "echo $'PRINT:5.3\n'\n";
        userData += "ls -l\n";
        userData += "rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm\n" +
                        "yum -y update\n" +
                        "yum -y install tesseract\n";
        userData += "sudo java -jar downloadWorker.jar " + workerNumber + "\n";

        return userData;
    }

    private int getNotActiveWorkerNumber(){

        for(int i=0; i<maxWorkers; i++){
            if(!activeWorkerNumbers[i]){
                return i;
            }
        }

        return -1;
    }

    private void wait5Sec(){
        try{
            TimeUnit.SECONDS.sleep(5);
        }
        catch(Exception e){
            System.err.println("ERROR:\n");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void tagInstancesAsWorkers(List<Instance> instances){
        Ec2Client ec2 = Ec2Client.create();

        for(Instance instance : instances){
            wait5Sec();
            Tag tag = Tag.builder().key("Name").value("worker").build();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instance.instanceId())
                    .tags(tag)
                    .build();

            ec2.createTags(tagRequest);
        }

        ec2.close();
    }

    private Runnable makeWorkerMessageRunnable(Message message){
        return () -> {
            //decode message in the form: queueUrl +  "\n" +  imageUrl +  "\n" +  text
            String[] decodedMessage = decodeMessageFromWorker(message.body());

            //get ImageTasks that one of his images have been processed
            ImageTasks imageTasks = tasks.get(decodedMessage[0]);

            //update hashmap
            try{
                imageTasks.updateTask(decodedMessage[1],decodedMessage[2]);
            }
            catch (Exception e){
                System.err.println(e.getMessage());
                System.exit(1);
            }

            //check if all of ImageTasks images have been processed
            if(imageTasks.isTaskCompleted()){
                taskFinished(imageTasks);
            }
            SQS_Service.getInstance().deleteMessage(message, workersToManagerQueueURL);
        };
    }

    private void taskFinished(ImageTasks tasks){
        String fileName = "clientsFiles/" + tasks.getKeyName();
        File targetFile = new File(fileName);
        targetFile.delete();
        File file = new File(fileName);

        try {
            file.getParentFile().mkdirs();
            file.createNewFile();
            PrintWriter out = new PrintWriter(new FileWriter(file, true));
            for(String key : tasks.getUrlText().keySet()){
                out.append(key).append("\n");
                out.append(tasks.getUrlResult(key)).append("\n\n");
            }
            out.close();
        }
        catch (IOException e) {
            System.err.println("ERROR: can't create a new file");
            e.printStackTrace();
            System.exit(1);
        }
        String location = tasks.getBucket() + "\n" + "answer_" + tasks.getKeyName();
        S3_Service.getInstance().uploadInputToS3(file, location);
        location = MessageMaker.makeLocation(location);
        if(checkAskForTermination()){
            location = MessageMaker.askTermination(location);
        } else{
            location = MessageMaker.askNoTermination(location);
        }
        SQS_Service.getInstance().sendMessage(tasks.getClientURL(), location);
        SQS_Service.getInstance().deleteMessage(tasks.getMessage(), tasks.getClientURL());
        boolean deleted = s3_service.deleteFromS3(tasks.getBucket() , tasks.getKeyName());
        removeTask(tasks.getClientURL());
    }

    private boolean checkAskForTermination(){
        terminate = shouldTerminate && tasks.keySet().size() == 1;
        return terminate;
    }

    //Read all messages from available un-read queues
    private void readAllMessages(LinkedList<String> urlNeedToServeQueues, int mode){
        while(!urlNeedToServeQueues.isEmpty()){
            //get queue's url
            String queueUrl = urlNeedToServeQueues.remove();

            Message message = readMessage(queueUrl);


            Runnable runnableMessage;

            if(mode == 1){
                runnableMessage = makeClientMessageRunnable(queueUrl, message);
                //If thread one is executing the function , need to update that client with the given queue's url is
                //being served ,and we should not wait for any message from him yet.
            }
            else{
                urlNeedToServeQueues.add(queueUrl);
                if(message == null){
                    return;
                }
                else{
                    runnableMessage = makeWorkerMessageRunnable(message);
                }
            }
            //submit runnable task to pool.
            pool.submit(runnableMessage);

        }
    }

    private void readAllMessagesFromWorkers(String queueURL){
        Runnable runnableMessage;
        Message message = readMessage(queueURL);

        while(message != null){
            runnableMessage = makeWorkerMessageRunnable(message);
            pool.submit(runnableMessage);
            message = readMessage(queueURL);
        }
    }

    //Thread2 only used-functions.
    private String[] decodeMessageFromWorker(String message){
        //message in the form of queueUrl + "\n" + imageUrl + "\n" + text;
        return message.split("\\n");
    }

    private void removeTask(String queueUrl){
        tasks.remove(queueUrl);
    }

    public Runnable thread2Serve(){
        return() ->{
            while(!terminate){
                readAllMessagesFromWorkers(workersToManagerQueueURL);
                try{
                    TimeUnit.SECONDS.sleep(5);
                }
                catch(Exception e){
                    System.err.println("ERROR:\n");
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
                terminateWorkerIfNeeded();
            }
        };
    }


    //Thread1 only used-functions.
    public Runnable thread1Serve() {
        return () ->{
            String prefix = "Client";
            while(!terminate){
                thread1servecounter++;
                //Get all queues whom available
                ListQueuesResponse ls = getAllQueuesResponse(prefix);
                cleanUrlsNoLongerToServe(ls);

                if(!shouldTerminate){
                    //Get list of urls of only need to be served queues
                    LinkedList<String> urlNeedToServeQueues = urlNeedToServeQueues(ls);
                    //2 Read , Make runnable , put in pool and update queue concurrentSqsUrlsClients
                    readAllMessages(urlNeedToServeQueues, 1);
                }
                else{
                    checkAskForTermination();
                }
            }
            onExit();
        };
    }

    private void closeAllWorkers(){
        Ec2Client ec2 = Ec2Client.create();
        String nextToken = null;
        boolean notManager = true;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(19).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for(Tag tag : instance.tags()){
                            if(tag.key().equals("Name") && tag.value().equals("manager")){
                                notManager = false;
                            }
                        }
                        if(notManager){
                            terminateInstance(ec2, instance.instanceId());
                        }
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
            for(int i=0 ;i<activeWorkerNumbers.length ; i++){
                activeWorkerNumbers[i] = false;
            }
        }
        catch (Ec2Exception e) {
            System.err.println("ERROR: can't find instances in EC2\n");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        ec2.close();
    }

    private void terminateInstance(Ec2Client ec2, String instanceId) {
        try{
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            TerminateInstancesResponse response = ec2.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        System.out.println("Successfully stopped instance " + instanceId);
    }

    private void onExit(){
        closeAllWorkers();
        SQS_Service.getInstance().deleteQueue(managerToWorkersQueueURL);
        SQS_Service.getInstance().deleteQueue(workersToManagerQueueURL);
        S3_Service.getInstance().deleteFromS3("yourfriendlyneighborhoodbucketman", "Manager.jar");
        S3_Service.getInstance().deleteFromS3("yourfriendlyneighborhoodbucketman", "Worker.jar");
        S3_Service.getInstance().deleteFromS3("yourfriendlyneighborhoodbucketman", "tessdata.zip");
        S3_Service.getInstance().closeClient();
        SQS_Service.getInstance().closeClient();
        pool.shutdown();
        mainPool.shutdown();
        System.out.println("server closed!!!");
    }

    private void terminateWorkerIfNeeded(){
        Ec2Client ec2 = Ec2Client.create();
        String nextToken = null;
        int workerNumber;
        try {
            do {
                DescribeInstancesRequest request = DescribeInstancesRequest.builder().maxResults(19).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for(Tag tag : instance.tags()){
                            if(checkValidWorkerNumber(tag.key()) && checkForTerminationRequest(tag.value()) && !instance.state().nameAsString().equals("terminated")){
                                workerNumber = Integer.parseInt(tag.key());
                                terminateInstance(ec2, instance.instanceId());
                                activeWorkerNumbers[workerNumber] = false;
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

    private boolean checkValidWorkerNumber(String workerNumber){
        for(int i=0; i<activeWorkerNumbers.length; i++){
            if(workerNumber.equals(String.valueOf(i))){
                return true;
            }
        }
        return false;
    }

    private boolean checkForTerminationRequest(String value){
        return value.equals("Terminate");
    }

}


