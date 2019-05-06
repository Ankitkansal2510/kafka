package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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


            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Ankit" + Integer.toString(i)); //

            //4  Then we send the data
            //In this we have not send the data to partition with keys so data will randomly go to any partition using round robin algorithm
            //The below send is asynchronous
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
            });
        }
        //this will flush the data
        producer.flush();

        //close the producer
        producer.close();
    }
}
