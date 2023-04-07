import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

public class ImageTasks {

    private final ConcurrentHashMap<String , String> url_text; // <url,text>
    private int imagesPending; // to know when we finish processing BigFile.
    private final String clientURL;
    private final String s3Location;
    private final Message message;

    public ImageTasks(File file, String _clientUrl, String location, Message _message){
        this.imagesPending = 0;
        this.url_text = new ConcurrentHashMap<>();
        clientURL = _clientUrl;
        readEachUrl(file);
        s3Location = location;
        message = _message;
    }

    public String getSize(){
        String output = "url_text = " + url_text.size() + "\nimagesPending = " + imagesPending;
        return output;
    }

    public String getBucket(){
        return MessageMaker.getBucket(s3Location);
    }

    public String getKeyName(){
        return MessageMaker.getKey(s3Location);
    }

    public String getClientURL(){
        return clientURL;
    }

    public ConcurrentHashMap<String, String> getUrlText(){
        return url_text;
    }

    public String getUrlResult(String key){
        if(url_text.containsKey(key))
            return url_text.get(key);
        return "Error occurred it ImageTasks:getUrlResult";
    }

    private void readEachUrl(File file){
        int numOfUrls = 0;
        List<String> urls = file2List(file);

        for(String url : urls){
            if(!url_text.containsKey(url))
                url_text.putIfAbsent(url, "");
        }

        numOfUrls = url_text.keySet().size();

        imagesPending = numOfUrls;
    }

    private List<String> file2List(File file){

        List<String> list = new ArrayList<>();

        try {
            Scanner reader = new Scanner(file);
            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                if(!line.equals(""))
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

    public void updateTask(String imageURL, String text) throws Exception {

        if(!isUrlInTask(imageURL)){
            throw new Exception("ERROR: Given URL is not in task!\n");
        }

        if(urlAlreadyProcessed(imageURL)){
            return;
        }

        url_text.replace(imageURL, text);

        imagesPending--;
    }

    public boolean isTaskCompleted(){
        return imagesPending == 0;
    }

    public boolean isUrlInTask(String url){
        return url_text.containsKey(url);
    }

    private boolean urlAlreadyProcessed(String url){
        return !url_text.get(url).equals("");
    }

    public Message getMessage() {
        return message;
    }
}
