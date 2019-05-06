package com.ankit.github.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class StreamFilterTweet {

    public static void main(String[] args) {
        //create properties
    Properties properties=new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");//it is similar to consumer group but for stream applicatoin
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()); //
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class.getName());

        //create a topology
      //  KStreamsBuilder streamsBuilder=new KStreamsBuilder();

        KStreamBuilder streamsBuilder=new KStreamBuilder();

        //input topic
        KStream<String,String> inputTopic=streamsBuilder.stream("Twitter_Tweets");
        KStream<String,String> filteredStream=inputTopic.filter(
            //filter for tweets which users have more thatn 10k follower
                (k,jsonTweet)->extractUserFrollowerFromTweet(jsonTweet)>10000
        );
        filteredStream.to("important_tweets");

        //build a topology

        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder,properties);


        //start our stream application
        kafkaStreams.start();
    }
    private static JsonParser jsonParser=new JsonParser();
    private static Integer extractUserFrollowerFromTweet(String tweetJson)
    {
        //use gson library for this
        try {
          return  jsonParser.parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }
        catch(NullPointerException e)
        {
        return 0;
        }
    }
}
