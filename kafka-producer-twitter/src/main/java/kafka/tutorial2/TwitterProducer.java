package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
    /**The below consumerKey ,consumerSecret , token and secret we have get from the twitter developer website
     * url is https://developer.twitter.com/en/apps/16338707  {Note :I have created the developer account using twitter credential}
     * Once logging to developer account go to keys and tokens link
     * then you will get the below information
     * Note:Once you regenerate the below 4 details ,update the newly generated details in the below 4 properties
     */
    String consumerKey="WydD0q5GzjAQGhZDvrdgVouDY";
    String consumerSecret="9avvSghPIwU2Owoyb5MVo0Hcvs37n1DQPERM7ppkHfWf5TdmE7";
    String token="1053359627398713345-x14awgxfXSBQE4H7dz04lMsnYbMhR9";
    String secret="R8uvX6FLqZESlmdxKV135umfFwfjFdNLrxq6zXD3eHqrQ";

    public TwitterProducer()
    {
    }

    public static void main(String[] args) {
    new TwitterProducer().run();
    }

    public void run()
    {
        logger.info("Setup");
        //message queue in which twitter tweets will store
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //create a twitter client
        Client client=createTwitterClient(msgQueue);
// Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String,String> producer=createProducer();

        //adding a shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {
            logger.info("Shutting down the application");
            logger.info("shutting down client from twitter");
            client.stop();
            logger.info("Closing producer");
            producer.close();
            logger.info("Done");

        }));

        //loop to send the tweet to kafka

        while (!client.isDone()) {
            String msg = null;
            try{
                //retrieving the tweets from blockingQueue and assinging to string variable msg
                msg= msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();;
            }
            if(msg!=null)
            {
                logger.info(msg);
                //producer is sending the tweets to kafka
                producer.send(new ProducerRecord<>("Twitter_Tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Receivedd new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset :" + recordMetadata.offset() + "\n" +
                                    "TimeStamp : " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                });
            }

        }
        logger.info("End of application");
    }



    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("Bitcoin","usa","politics","IPL","cricket","football");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                                   // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public KafkaProducer<String,String> createProducer()
    {
        Properties properties=new Properties();
        String bootstrapServers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Key serializer and value serializer basically help producer know what type of value is send to kafka and how this can be send serialize
        // to bytes.because Kafka client converts whatever we sent to kafka into  bytes 0 and 1

        /**Now we are creating safe producer
         * setting acks=all,retries >1 and also setting the idempotedProducer
         */
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");//This setting will enable the idempotence behavior
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all"); //This setting is used to set acks=all
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE)); //This setting is used to tell producer how many retires need to be done if first attempt of writing data to kafka fails
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");//This setting is used to send parallel request can be made.

        /**Below properties we are setting to increase the high throughput of the
         * producer (at the expense of bit of latency and cpu usage
         */

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");//Using the compression alogrithm "snappy",Snappy is developed by Google and it is very fast compression algorithm
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20"); //This setting will allow producer to wait for 20 millisecond so that it can collect all the data within 20 ms and sends them all together in a batch
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));//This setting is used to set batch size,here we are setting 32kb


        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }
}
