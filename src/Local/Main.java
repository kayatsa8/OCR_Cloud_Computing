import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.io.File;
import java.util.*;

import software.amazon.awssdk.services.sqs.model.Message;


public class Main {

    public static void main(String[] args){

        /*
         * 1) read input + put input in S3 V
         *  1.1) credentials are necessary.
         * 2) check if manager on, else turn it on V
         * 3) notify manager V
         * 4) wait to manager's answer
         *   4.1) busy-wait to manager
         *   4.2) get location of answer in S3
         *   4.3) parse the message + take output from S3
         *   ## ASSUMPTION: message pattern is: bucketName "\n" key
         * 5) create html contains the answer
         *
         *
         * special case: terminate manager
         */
        if(args.length >= 3){
            Client client = new Client(args[0], args[1], args[2]);
            if(args.length == 4){
                client.askForTermination(args[3]);
            }
            client.run();
            System.out.println("\nFINISHED!\n");
        }
    }

}
