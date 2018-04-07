import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Tuple2;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws InterruptedException, ConfigurationException {

//        Cassandra driver = new Cassandra();
//        driver.createKeyspace("twitter");
//        driver.createTable("twitter", "tweet");

        final String[] keyfacebookwords = {"Euphoria"};
        final String keyword = "Euphoria";

//        TwitterStatus status = new TwitterStatus();
//        status.scanStatus(keywords);

        SparkJob job = new SparkJob();
//        job.task1(keyword);
//        job.task3(keyword, 10);


        CompositeConfiguration conf = new CompositeConfiguration();
        conf.addConfiguration(new PropertiesConfiguration("spark.properties"));

        try {
            job.realTask1("#facebook", conf);
        } catch (Exception e) {
            e.printStackTrace();
        }



    }


}




//        JSONObject json = new JSONObject();
//        JSONParser parser = new JSONParser();
//        JSONObject jsonObject = null;
//
//        Object obj = null;
//        try {
//            obj = parser.
//                    parse(new FileReader("/home/konrad/Pobrane/eestechchallenge-master/batchprocessing/src/main/java/languages.json"));
//            jsonObject = (JSONObject) obj;
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//
//
//
//        String x = (String) jsonObject.get("code").toString();
//
//        System.out.println(x);
