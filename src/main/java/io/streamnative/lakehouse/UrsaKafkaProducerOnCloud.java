package io.streamnative.lakehouse;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.streamnative.tieredstorage.test.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class UrsaKafkaProducerOnCloud {
    public static void main(String[] args) throws Exception {
        List<String> topics = new ArrayList<>();
        String tp = "test_v3";
        topics.add(tp);

        // The service URL, schemaRegistryUrl and API Key can be found in the StreamNative Console
        final String serverUrl = "serviceUrl";
        final String schemaRegistryUrl = "<schemaRegistryUrl>";
        String jwtToken = "<jwtToken>";
        final String token = "token:" + jwtToken;

        final Properties producerProps = new Properties();
        producerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        producerProps.put("sasl.mechanism", "PLAIN");
        // Note: You can use any username for the `sasl.username` because only the token will be used for authentication
        producerProps.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"%s\";",
                token));

        // Set configurations for schema registry
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        producerProps.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        producerProps.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, String.format("%s:%s", "public", token));
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        final int totalMessages = 1000000;

        final KafkaProducer<String, Student> producer = new KafkaProducer<>(producerProps);
        for (int i = 0; i < totalMessages; i++) {
            log.info("produced: {}", i);
            Student student = new Student();
            student.setName("name" + i);
            student.setAge(i);
            student.setAddress("address" + i);
            student.setPayload("payload" + i);
            student.setNumber(i);
            for (String topic : topics) {
                producer.send(new ProducerRecord<>(topic, null, student));
            }

            if (i % 10000 == 0) {
                producer.flush();
            }
        }
        producer.flush();
        producer.close();
    }
}
