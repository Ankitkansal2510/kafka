package com.github.ankit.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient()
    {
        /**This method createClinet is used to make a connection with the elastic search which i have setup in the elastic search cloud using the url
         * https://app.bonsai.io/clusters/kafka-course-8727378731/console
         * logged in to this url and goto credential link in that page you will find the hostname ,username and password
         * Hostname is :https://yw2vzwxifv:xd4kziidae@kafka-course-8727378731.eu-west-1.bonsaisearch.net
         * but we will only use kafka-course-8727378731.eu-west-1.bonsaisearch.net,this may be change or not if in case i logged in again,if change then update the below hostname with the latest one
         */
        String hostname="kafka-course-8727378731.eu-west-1.bonsaisearch.net";
        String username="yw2vzwxifv";
        String password="xd4kziidae";

        final CredentialsProvider credentialsProvider=new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder= RestClient.builder(
                new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String,String> createConsumer(String topic)
    {
        String bootstrapServers="127.0.0.1:9092";
        String groupId="kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //we have three option "earliest","latest","none" same like from beginning ,latest message
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100"); //this property will make sure that 100 records will come at a time and then processed by consumer and then again 10 records come and so on
        //Create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }
    private static JsonParser jsonParser=new JsonParser();
    private static String extractIdFromTweet(String tweetJson)
    {
        //use gson library for this
     return   jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client=createClient();


        /** we are first creating the indexes /twitter/tweets in the elastic search that we have setup in the cloud "https://app.bonsai.io/clusters/kafka-course-8727378731/console"
         * username to login to elastic search cloud is kansal920@gmail.com and password is ankit25****
         * After indexes and type is created then we are entering the json data into it.
         * When this code will run it will return the id and using logger we are printing the id in the console
         * once the id is printed then go to elastic search cloud website using urs(https://app.bonsai.io/clusters/kafka-course-8727378731/console)
         * selet get method and in the input box type /twitter/tweets/id    Note id: is the id which is printed in the console
         * once you done you can see the data in the elastic search
         */
        /**String jsonString="{\"foo\":\"bar\"}";

        IndexRequest indexRequest=new IndexRequest(
                "twitter",
                "tweets"
        ).source(jsonString, XContentType.JSON);
        IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
        String id=indexResponse.getId();
        logger.info(id);

         We have commented above code and put it below inside consumer record because now we are consuming the data from the topic "twitter_tweet (Remember in the twitter producer
         we have created the topic twitter producer and produces the tweets into the kafka,now we have to consume the data from the kafka and insert the data into elasticsearch
         that is the reason we have created the kafkaconsumer **/

        KafkaConsumer<String,String> consumer=createConsumer("Twitter_Tweets");
        while(true)
        {
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100));
            Integer recordscount=records.count();
            logger.info("Received " + recordscount + " records");
            BulkRequest bulkRequest=new BulkRequest();

            //below for loop is reading one message and commiting which is slow , to improve the consumer performance we will use the bulkrecord
            for(ConsumerRecord<String,String> record:records) {
                //2 strategies to make unique id so that same message will not store multiple times

                //1st strategy is
                // String id=record.topic() +"_"+ record.partition() + "_"+record.offset();

                //2nd strategy is getting twitter feed id i.e every tweets have its id so we can use that as id
                try {
                    String id = extractIdFromTweet(record.value());
                    //here we insert data into elastic search

                    //This index and type are used like in the elastic search url mentoined above go to console select method get and in the inbox type
                    ///twitter/tweets/id
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id //this is to make sure consumer idempotent because if we run the same program again then same tweet will be inserted again in elastic search meaning duplicate data
                    ).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);//here we are adding the record in the bulk ,so now below two lines of code which is 144 and 145 is not needed so commenting it out
               /* IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());*/
                    //  String id_unique=indexResponse.getId(); //generating id this way will generate unique id everytime ,so if we run this program again it will generate the unique id because of which in the elastic search consumer will insert the same data duplicate                logger.info(id);
                }
                catch(NullPointerException e){
                    logger.warn("Skiiping bad data whose twitter id is null :" + record.value());
                }
            }
            if(recordscount>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Commiting the offsets");
                consumer.commitSync(); //this is used to commit the offset
                logger.info("Offset committed");
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
       // client.close();
    }
}
