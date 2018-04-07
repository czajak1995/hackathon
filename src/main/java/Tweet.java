import java.io.Serializable;

public class Tweet implements Serializable{

    public Tweet(String timestamp, String place,  String text, String userName, String language) {
        this.timestamp = timestamp;
        this.userName = userName;
        this.place = place;
        this.text = text;
        this.language = language;
    }

    String timestamp;

    String userName;

    String place;

    String text;

    String language;

}
