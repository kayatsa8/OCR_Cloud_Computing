

public class Main {
    public static void main(String[] args){
        /*
        * Main thread One executes - 3 - We can think of step 3 as "Handle accept"
        *
        *
        * 3.Retrieve all queses name that correspond with clients
        *   3.1 for each queue's name check if message has already been read off her (concurrent list).
        *       3.1.1 If not: a)get queue's url
        *                           * b) Read message
        *                                   * c)make it Runnable1
        *                                       * d)put in ActorThreadPool's mission's queue
        *                                           * e)add queue's name to the list of read queses
        *       3.1.2 If yes: do nothing
        *   3.2 Return to (3) as long as it is not interrupted (need to define that)
        *
        * 4. In handleReadWrite we submit the message to ActorThreadPool , this step and what it does need to be defined in the run function of the
        *       Runnable1 object we pass to handleReadWrite.
        * 5. Worker step - not in this project.
        *
        *
        * 6.Retrieving messages from WorkersToManagerQueue
        *   6.1 make it Runnable2
        *   6.2 put in ActorThreadPool's mission's queue (aka handleReadWrite)
        *   6.3 return to (6) as long as it is not interrupted (need to define that)
        *
        * 7. In handleReadWrite we submit the message to ActorThreadPool , this step and what it does need to be defined in the run function of the
        *       Runnable2 object we pass to handleReadWrite.
        *
        * 8.Client step - not in this project.
        * * * * * */

        System.out.println("Manager is on!\n");
        Manager manager = new Manager(10);
        manager.run();
        System.out.println("Done!");


    }
}
