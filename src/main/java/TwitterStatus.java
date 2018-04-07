import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
//import org.json.simple.JSONObject;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.xml.Null;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStatus {

    Cassandra driver = new Cassandra();
    String country = new String();
    String user = new String();
    String text = new String();
    String language = new String();
    Place place;


    final Configuration configuration = new ConfigurationBuilder()
            .setDebugEnabled(true)
            .setOAuthConsumerKey("YJVnzfSL6IDfButlqLt3j3fG6") // paste your consumer key
            .setOAuthConsumerSecret("KCTrfyi7jJXO3U2FDdE6awduFuBDwrS01LxsCIC2UwyPLEvdBu") //paste your CONSUMER_SECRET
            .setOAuthAccessToken("981940077369745410-QvtdYbmmuzkbkVJxbDOltq7WybGKTgv") //paste your OAUTH_ACCESS_TOKEN
            .setOAuthAccessTokenSecret("fVTUFqdbKM9vclIoOo9PqkrUP5olOmxxqIHUgMHTvCkhY") // paste your OAUTH_ACCESS_TOKEN
            .build();

    JSONObject jsonObject;

    public TwitterStatus() {
    }

    public void scanStatus(String ... keyword) {
        StatusListener listener = new StatusListener() {



            public void onStatus(Status status) {


                text = status.getText();

                try {
                    user = status.getUser().getName();
                }
                catch (NullPointerException e) {
                    user = "Undefined";
                }


                try {
                    place = status.getPlace();
                    country = place.getCountry();
                }
                catch (NullPointerException e) {
                    country = "Undefined";
                }

                try {
                    language = status.getLang();
                    JSONObject obj = (JSONObject) jsonObject.get("languages");
                    System.out.println(obj.toString());
                } catch (NullPointerException e) {
                    language = "Unknown";
                }

                driver.insertIntoTable("twitter", "tweet",
                        new Tweet(String.valueOf(System.currentTimeMillis()), user, country, text, language));

            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        TwitterStream twitterStream = new TwitterStreamFactory(configuration).getInstance();
        twitterStream.addListener(listener);
        twitterStream.filter(new FilterQuery().track(keyword));
    }

}
