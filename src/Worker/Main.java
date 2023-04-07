import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.LinkedList;

public class Main {

    /**
     * Each worker is EC2 instance
     * message in ManagerToWorkers are of the form: queueUrl + "\n" + imageUrl
     * @inv args[0] = workerNumber
     * NOTE: originally args[1] (should've been args[0]) contained number of tasks to work on
     */

    public static void main(String[] args){
        System.out.println("Worker");
        Worker worker = new Worker(Integer.parseInt(args[0]));
        worker.workerServe();
        System.out.println();
    }

}
