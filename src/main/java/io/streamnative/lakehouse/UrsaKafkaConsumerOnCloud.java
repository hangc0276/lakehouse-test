package io.streamnative.lakehouse;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.streamnative.tieredstorage.test.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class UrsaKafkaConsumerOnCloud {
    public static void main(String[] args) throws Exception {

        // The service URL, schemaRegistryUrl and API Key can be found in the StreamNative Console
        final String serverUrl = "serviceUrl";
        final String schemaRegistryUrl = "<schemaRegistryUrl>";
        String jwtToken = "<jwtToken>";
        final String token = "token:" + jwtToken;

        final String topicName = "test_v3";
        final String groupId = "test_sub_v1";

        // 1. Create a consumer with token authentication, which is equivalent to SASL/PLAIN mechanism in Kafka
        // Note: You can use any username for the `sasl.username` because only the token will be used for authentication
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"%s\";",
                token));

        // Set configurations for schema registry
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG, String.format("%s:%s", "public", token));

        log.info("create consumer");
        // create consumer
        KafkaConsumer<String, Student> consumer = new KafkaConsumer<>(props);

        log.info("start to subscribe topic: {}", topicName);
        // subscribe topic
        consumer.subscribe(Arrays.asList(topicName));

        log.info("start to consumer message...");
        int cnt = 0;
        while (true) {
            ConsumerRecords<String, Student> students = consumer.poll(Duration.ofMillis(1000));
            log.info("record size: {}, cnt: {}", students.count(), cnt);
            for (ConsumerRecord<String, Student> record :students) {
                cnt++;
                log.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(), record.value(), record.partition(), record.offset());
            }
        }
    }
}
