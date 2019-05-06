package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
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

        ProducerRecord<String,String> record=new ProducerRecord<String, String>( "first_topic","Bingo my first java Producer"); //

        //4  Then we send the data

        producer.send(record);
        //this will flush the data
        producer.flush();

        //close the producer
        producer.close();
    }
}
