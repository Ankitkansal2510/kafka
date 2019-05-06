package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


//In this class we have not specified the group id but we have tell the consumer that from which partition
//and from which offset consumer needs to consume message also we have put the condition that it should only read 5 messages
public class ConsumerDemoGroupAssignSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupAssignSeek.class);
        String bootstrapServers="127.0.0.1:9092";

        String topic="first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //we have three option "earliest","latest","none" same like from beginning ,latest message

        //Create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

       //assign and seek are mostly use to fetch a specific message
        TopicPartition partitiontoReadFrom=new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitiontoReadFrom));
    long offsettoReadFrom=15L;
        consumer.seek(partitiontoReadFrom,offsettoReadFrom);


        //poll for new data

        int numberfromMEssagestoREad=5;
        boolean keepOnreading=true;
        int numberofMessagesReadSoFar=0;


        while(keepOnreading)
        {
          ConsumerRecords<String,String> records=
                  consumer.poll(Duration.ofMillis(100));
          for(ConsumerRecord<String,String> record:records)
          {
              numberofMessagesReadSoFar+=1;
              logger.info("Key :" +  record.key() + " value: " + record.value());
              logger.info("Partition : " +record.partition() + " Offset : " + record.offset());
                if(numberofMessagesReadSoFar>=numberfromMEssagestoREad)
                {
                    keepOnreading=false;
                    break;
                }
          }
        }
logger.info("Exiting the application");
    }
}