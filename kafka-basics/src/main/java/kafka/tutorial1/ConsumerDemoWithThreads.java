package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }
    private   ConsumerDemoWithThreads()
    {
    }
    private void run()
    {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServers="127.0.0.1:9092";
        String groupId="my-fifth-application";
        String topic="first_topic";

        CountDownLatch latch=new CountDownLatch(1);

        logger.info(("Creating thread"));
        Runnable consumerRunnable=new ConsumerThreads(bootstrapServers,groupId,topic,latch);
        Thread myThread=new Thread(consumerRunnable);
        myThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted",e);
        }
        finally {
            logger.info("Application is closing");
        }

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( ()->{
            logger.info("Caught shutdown hook");
            ((ConsumerThreads)  consumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        logger.info("Appliation has exited");
        }
        ));
    }

    public class ConsumerThreads implements Runnable{
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String,String> consumer;
       private  Logger logger = LoggerFactory.getLogger(ConsumerThreads.class);

        public ConsumerThreads(String bootstrapServers,String groupId,String topic,CountDownLatch countDownLatch)
        {

            this.countDownLatch=countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //we have three option "earliest","latest","none" same like from beginning ,latest message

            consumer=new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }
        public void run()
        {
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key :" + record.key() + " value: " + record.value());
                        logger.info("Partition : " + record.partition() + " Offset : " + record.offset());

                    }
                }
            }
            catch(WakeupException e)
            {
            logger.info("ShutDown signal!");
            }
            finally {
                consumer.close();
                //Tell the main code we are done with the consumer
                countDownLatch.countDown();
            }
        }
        public void shutDown()
        {
            //Special method to interupt consumer.poll
            //It will throw exception WakeUpException
            consumer.wakeup();

        }
    }
}