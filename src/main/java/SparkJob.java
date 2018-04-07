import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import scala.collection.mutable.HashMap;
import shade.com.datastax.spark.connector.google.common.collect.Lists;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.conf.ConfigurationContext;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

public class SparkJob implements Serializable{


    public boolean relativeTimestamp(long time1) {
        long res = currentTimeMillis() - time1;
        return res < 3600*24*7*1000;
    }


    public void task1(String keyword) {
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.setAppName("twitter-hackaton-mashup");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.cassandra.connection.host", "localhost");
        sparkConf.set("spark.cassandra.connection.port", "9042");
        SparkContext sparkContext = new SparkContext(sparkConf);


        CassandraTableScanJavaRDD<CassandraRow> rdd = CassandraJavaUtil.javaFunctions(sparkContext)
                .cassandraTable("twitter", "tweet");

        List<Tweet> users = rdd
                .map(x -> (new Tweet(x.getString("id"),
                        x.getString("country"),
                        x.getString("tweet_text"),
                        x.getString("user_name"),
                        x.getString("language"))))
                .filter(x -> relativeTimestamp(Long.parseLong(x.timestamp)) )
                .filter(x -> x.text.toLowerCase().contains(keyword.toLowerCase()))
                .collect();

        System.out.println("SIZE " + users.size());

        users.stream().forEach(x -> {
                System.out.println(x.userName);
        });

    }


    public void realTask1(String keyword, CompositeConfiguration conf) throws InterruptedException {
        final Configuration configuration = new ConfigurationBuilder()
                .setDebugEnabled(true)
                .setOAuthConsumerKey("YJVnzfSL6IDfButlqLt3j3fG6") // paste your consumer key
                .setOAuthConsumerSecret("KCTrfyi7jJXO3U2FDdE6awduFuBDwrS01LxsCIC2UwyPLEvdBu") //paste your CONSUMER_SECRET
                .setOAuthAccessToken("981940077369745410-QvtdYbmmuzkbkVJxbDOltq7WybGKTgv") //paste your OAUTH_ACCESS_TOKEN
                .setOAuthAccessTokenSecret("fVTUFqdbKM9vclIoOo9PqkrUP5olOmxxqIHUgMHTvCkhY") // paste your OAUTH_ACCESS_TOKEN
                .build();
        Authorization twitterAuth = AuthorizationFactory.getInstance(configuration);

        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkCrawler").
                setMaster(conf.getString("spark.master"))
                .set("spark.serializer", conf.getString("spark.serializer"));
        JavaStreamingContext jssc =
                new JavaStreamingContext(sparkConf, Durations.seconds(conf.getLong("stream.duration")));
        String[] filters = { keyword };

        JavaDStream<Status> status = TwitterUtils.createStream(jssc, twitterAuth, filters);//.print();

        status.foreachRDD((Function<JavaRDD<Status>, Void>) rdd -> {
            rdd.filter(x -> x.getText().contains(keyword))
                    .foreach((VoidFunction<Status>) s -> {
                System.out.println(s.getText() + "\n\n");

            });
            return null;
        });


        jssc.start();
        jssc.awaitTermination();
    }

    public void realTask3(String keyword, CompositeConfiguration conf) throws InterruptedException {
        final Configuration configuration = new ConfigurationBuilder()
                .setDebugEnabled(true)
                .setOAuthConsumerKey("YJVnzfSL6IDfButlqLt3j3fG6") // paste your consumer key
                .setOAuthConsumerSecret("KCTrfyi7jJXO3U2FDdE6awduFuBDwrS01LxsCIC2UwyPLEvdBu") //paste your CONSUMER_SECRET
                .setOAuthAccessToken("981940077369745410-QvtdYbmmuzkbkVJxbDOltq7WybGKTgv") //paste your OAUTH_ACCESS_TOKEN
                .setOAuthAccessTokenSecret("fVTUFqdbKM9vclIoOo9PqkrUP5olOmxxqIHUgMHTvCkhY") // paste your OAUTH_ACCESS_TOKEN
                .build();
        Authorization twitterAuth = AuthorizationFactory.getInstance(configuration);

        SparkConf sparkConf = new SparkConf().setAppName("TwitterSparkCrawler").
                setMaster(conf.getString("spark.master"))
                .set("spark.serializer", conf.getString("spark.serializer"));
        JavaStreamingContext jssc =
                new JavaStreamingContext(sparkConf, Durations.seconds(conf.getLong("stream.duration")));
        String[] filters = { keyword };

        DStream<Tuple2<String, Integer>> status =
                TwitterUtils.createStream(jssc, twitterAuth, filters)
                .map(x -> (new Tuple2<String, Integer>(x.getText(), x.getUser().
                        getFollowersCount()))).dstream();//.print();

        String name = status.toString();


        jssc.start();
        jssc.awaitTermination();
    }


    public void task3(String keyword, int amount) {
        SparkConf sparkConf = new SparkConf(true);
        sparkConf.setAppName("twitter-hackaton-mashup");
        sparkConf.setMaster("local[*]");
        sparkConf.set("spark.cassandra.connection.host", "localhost");
        sparkConf.set("spark.cassandra.connection.port", "9042");
        SparkContext sparkContext = new SparkContext(sparkConf);


        CassandraTableScanJavaRDD<CassandraRow> rdd = CassandraJavaUtil.javaFunctions(sparkContext)
                .cassandraTable("twitter", "tweet");

        List<Tweet> users = rdd
                .map(x -> (new Tweet(x.getString("id"),
                        x.getString("country"),
                        x.getString("tweet_text"),
                        x.getString("user_name"),
                        x.getString("language"))))
                .filter(x -> x.language.equals("en"))
                .filter(x -> x.text.toLowerCase().contains(keyword.toLowerCase()))
                .collect();

        System.out.println("SIZE " + users.size());

        JavaPairRDD<String, Integer> pairRDD = rdd.groupBy(new Function<CassandraRow, String>() {
            @Override
            public String call(CassandraRow cassandraRow) throws Exception {
                return cassandraRow.getString("language");
            }
        }).mapToPair(new PairFunction<Tuple2<String,Iterable<CassandraRow>>, String, Integer>() {

            @Override

            public Tuple2<String, Integer> call(Tuple2<String, Iterable<CassandraRow>> t) throws Exception {

                return new Tuple2<String,Integer>(t._1(), Lists.newArrayList(t._2()).size());

            }

        });

        pairRDD.cache();

        List<Tuple2<String, Integer>> tuples = pairRDD.collect();

        tuples = tuples.stream().sorted(Comparator.comparing(x -> -x._2)).
                collect(Collectors.toList());


        for (int i = 0; i < amount; i++) {
            System.out.println(tuples.get(i));
        }
    }

}