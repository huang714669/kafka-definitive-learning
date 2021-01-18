package com.zihao.learnkafka.ch03.producer;

import com.zihao.learnkafka.ch03.producer.DemoProducerCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/1/15 下午5:07
 */
public class FirstProducer {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        // declare the properties of cluster and information about data key and value
        kafkaProps.put("bootstrap.servers", "172.23.130.35:9092,172.23.130.35:9093,172.23.130.35:9094");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // Create producer with the above properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
//        simpleSend(producer);
//        syncSend(producer);
          asyncSend(producer);
          producer.close();
    }

    // create a simple producer
    private static void simpleSend(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "london");
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步发送消息。　ｋａｆｋａ一般会发生两类错误：
     * １. 克重试错误，通过再次建立连接来解决
     * 2.　无主错误，通过重新分区选举解决
     */

    private static void syncSend(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "England");
        try {
            producer.send(record).get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步发送消息， 通过回调实现
     */
    private static void asyncSend(KafkaProducer<String, String> producer) {
        ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
        producer.send(record, new DemoProducerCallback());
    }
}


