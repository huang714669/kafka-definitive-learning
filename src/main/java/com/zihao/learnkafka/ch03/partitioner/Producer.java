package com.zihao.learnkafka.ch03.partitioner;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/1/18 下午4:32
 */
public class Producer {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        // declare the properties of cluster and information about data key and value
        kafkaProps.put("bootstrap.servers", "172.23.130.35:9092,172.23.130.35:9093,172.23.130.35:9094");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 设置自定义分区器
        kafkaProps.put("partitioner.class", "com.zihao.learnkafka.ch03.partitioner.BananaPartitioner");

        kafkaProps.put("acks", "all");

        // Create producer with the above properties
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps);

        // 创建普通消息的ProducerRecord
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                "sun", "s1", "cn dota best dota");

        // 发送普通消息
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            int partition = recordMetadata.partition();
            System.out.println(recordMetadata + ":" + partition);
            e.printStackTrace();
        });

        // 创建Banana消息的ProducerRecord，把键设置为Banana
        ProducerRecord<String, String> bananaRecord = new ProducerRecord<>(
                "sun", "Banana", "cn banana best banana");

        // 发送普通消息
        kafkaProducer.send(bananaRecord, (recordMetadata, e) -> {
            int partition = recordMetadata.partition();
            System.out.println(recordMetadata + ":" + partition);
            e.printStackTrace();
        });

        kafkaProducer.flush();
    }
}
