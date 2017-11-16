package gratinalfi.twitter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

/**
 * Connects to twitter and streams the results to a file
 */
public class PrintTwitterStream {

	public static void main(String[] args) throws IOException, InterruptedException {
		System.setProperty("twitter4j.oauth.consumerKey", "xxxx");
		System.setProperty("twitter4j.oauth.consumerSecret", "xxxx");
		System.setProperty("twitter4j.oauth.accessToken", "xxxx");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "xxxx");
		

		SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("BusProcessor");
		JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(5000));
		String[] query = {"#irelandsgreatest"};
		JavaDStream<Status> tweets = TwitterUtils.createStream(sc, null, query);
		JavaDStream<String> x = tweets.map(s -> s.getText());
		JavaPairDStream<String, String> t = tweets
				.mapToPair(s -> new Tuple2<String, String>(s.getUser().getName(), s.getText()));
		t.saveAsHadoopFiles("out/tweets.txt", "txt", Text.class, IntWritable.class, TextOutputFormat.class);

		t.print();
		sc.start();
		sc.awaitTermination();
	}
}
