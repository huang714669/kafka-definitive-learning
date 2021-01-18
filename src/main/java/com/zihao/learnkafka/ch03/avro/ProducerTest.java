package com.zihao.learnkafka.ch03.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author hzh
 * @version 1.0
 * @date 2021/1/18 下午2:24
 */
public class ProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();

        // declare the properties of cluster and information about data key and value
        kafkaProps.put("bootstrap.servers", "172.23.130.35:9092,172.23.130.35:9093,172.23.130.35:9094");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");

        // Create producer with the above properties
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(kafkaProps);
        for (int i = 0; i < 1000 ; i++) {
            Customer customer = new Customer();
            customer.setId(i);
            customer.setName("hzh" + i);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, (BinaryEncoder)null);
            SpecificDatumWriter<Object> writer = new SpecificDatumWriter<>(customer.getSchema());
            try {
                writer.write(customer, encoder);
                encoder.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            ProducerRecord<String,byte[]> record = new ProducerRecord<String, byte[]>("Customer","customer-"+i,out.toByteArray());
            producer.send(record);
        }
        producer.close();
    }
}
