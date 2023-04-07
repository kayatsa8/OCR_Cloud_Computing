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

    public static String makeLocation(String bucketName, String key, String numberOfUrlsForWorker){
        return bucketName + "\n" + key + "\n" + numberOfUrlsForWorker + "\n" + "0";
    }

    public static String getBucket(String location){
        String[] str = location.split("\\n");

        return str[0];
    }

    public static String getKey(String location){
        String[] str = location.split("\\n");

        return str[1];
    }

    public static String makeLocation_terminate(String bucketName, String key, String numberOfUrlsForWorker){
        return bucketName + "\n" + key + "\n" + numberOfUrlsForWorker + "\n" + "1";
    }

    public static boolean shouldTerminateManager(String location){
        String[] str = location.split("\\n");

        return str[2].equals("Terminate");
    }

}
