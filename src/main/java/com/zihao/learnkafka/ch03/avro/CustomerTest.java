package com.zihao.learnkafka.ch03.avro;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/1/18 下午3:31
 */
public class CustomerTest {
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        // declare the properties of cluster and information about data key and value
        kafkaProps.put("bootstrap.servers", "172.23.130.35:9092,172.23.130.35:9093,172.23.130.35:9094");
        kafkaProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put("group.id","DemoAvroKafkaConsumer2");
        kafkaProps.put("auto.offset.reset","earliest");

        KafkaConsumer<String ,byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

        consumer.subscribe(Collections.singletonList("Customer"));

        SpecificDatumReader<Customer> reader = new SpecificDatumReader<>(Customer.getClassSchema());
        try {
            while (true){
                ConsumerRecords<String,byte[]> records = consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record : records){
                    Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                    try {
                        Customer customer = reader.read(null,decoder);
                        System.out.println(record.key() + ":" + customer.get("id") + "\t" + customer.get("name"));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
