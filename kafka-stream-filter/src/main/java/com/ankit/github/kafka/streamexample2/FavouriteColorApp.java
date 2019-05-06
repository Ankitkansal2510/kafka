package com.ankit.github.kafka.streamexample2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import sun.security.krb5.internal.tools.Ktab;

import java.util.Arrays;
import java.util.Properties;

public class FavouriteColorApp {
    public static void main(String[] args) {


        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-java");//it is similar to consumer group but for stream applicatoin
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");//it is similar to consumer group but for stream applicatoin
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()); //
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");//WE disable cache to demonstrate all the steps involved in transformation ,not recommended in production

            KStreamBuilder builder=new KStreamBuilder();

            KStream<String,String> textLines=builder.stream("favourite-color-input");

            KStream<String,String> usersAndColours=textLines
                    // ensure that comma is here as well we will split on it
                    .filter((key,value)->value.contains(","))
                    //.we will select a key that will be the user id and convert it to lower case for safety purpose
                    .selectKey((key,value)->value.split(",")[0].toLowerCase())
                    //we will get color from the value and conver it to a lower case
                    .mapValues(value->value.split(",")[1].toLowerCase())
                    //we will filter undesired color
                    .filter((user,color)-> Arrays.asList("green","blue","red").contains(color));
                    //we send the kstream to output topic
                    usersAndColours.to("user-keys-and-colors");
    //read the topic as a ktable so update happends properly
        KTable<String,String> userAndColorTables=builder.table("user-keys-and-colors");
        //count the occurance of color
        KTable<String,Long> favouriteColor=userAndColorTables.groupBy((user,color)->new KeyValue<>(color,color)).count("CountsByColours");
        //send the data from ktable to output topic
        favouriteColor.to(Serdes.String(),Serdes.Long(),"favourite-colour-output");

        KafkaStreams streams=new KafkaStreams(builder,properties);
        streams.start();

        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));







    }
}