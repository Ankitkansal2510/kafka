package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithKeys.class);

      //Three steps to start a producer

        //1 To create a producer property
        Properties properties=new Properties();
        String bootstrapServers="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Key serializer and value serializer basically help producer know what type of value is send to kafka and how this can be send serialize
        // to bytes.because Kafka client converts whatever we sent to kafka into  bytes 0 and 1

        //2  then we create the producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);//Created the kafka producer

        //Create producer record
        for(int i=0;i<=10;i++) {

            String topic="first_topic";
            String value="Hello world with key";
            String key= "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value ); //

            //4  Then we send the data
            //In this we will  send the data to partition with keys so data will not randomly go to any partition using round robin algorithm

            logger.info("key: " + key);
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime the record has been sent or exception is throw
                    //We can either simple use producer.send(recod) but if we want more information like the name of the topic
                    //in which  partitioin the data is producing ,offset for the partition ,timestamp etc so we need to pass the
                    //extra paramter in the producer.send method that is new Callback and define the property as we have define below

                    if (e == null) {
                        logger.info("REceivedd new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset :" + recordMetadata.offset() + "\n" +
                                "TimeStamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get();//this .get will block send to make it synchronous but not follow this practise ,the reason we are making it as synchronus because to verify that keys behavious are working properly
        }
        //this will flush the data
        producer.flush();

        //close the producer
        producer.close();
    }
}
