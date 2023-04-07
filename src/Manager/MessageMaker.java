public class MessageMaker {

    /**
     * KEY FORMAT: <bucket name> \n <key> \n <number of urls for worker> \n <0 || 1>
     *     0 - don't terminate
     *     1 - terminate
     */

    /**
     * Answer format:
     * <bucket name> \n <key>_answer \n <Terminate || noTerminate>
     */

    public static String getBucket(String location){
        String[] str = location.split("\\n");

        return str[0];
    }

    public static String getKey(String location){
        String[] str = location.split("\\n");

        return str[1];
    }

    public static boolean shouldTerminate(String location){
        String[] str = location.split("\\n");

        return str[3].equals("1");
    }

    public static String makeLocation(String location){
        // location of an answer format: <bucket>\n<key>_answer
        // bucket and key are the original location given by the client
        return getBucket(location) + "\n" + getKey(location);
    }

    public static int getNumberOfUrlsForWorker(String location){
        String[] str = location.split("\\n");

        return Integer.parseInt(str[2]);
    }

    public static String askTermination(String location){
        location += "\nTerminate";
        return location;
    }

    public static String askNoTermination(String location){
        location += "\nnoTerminate";
        return location;
    }



}
