package kafka;

import com.arangodb.entity.BaseDocument;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.springframework.kafka.support.serializer.JsonDeserializer;


import java.util.Arrays;

import java.util.Collections;
import java.util.Properties;


public class SampleConsumer {

    public SampleConsumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, BaseDocument> kafkaConsumer = new KafkaConsumer<>(properties, new StringDeserializer(), new JsonDeserializer<>(BaseDocument.class));
        kafkaConsumer.subscribe(Arrays.asList("test"));

        TopicPartition topicPartition = new TopicPartition("test", 0);

        while (true) {
            ConsumerRecords<String, BaseDocument> records = kafkaConsumer.poll(1000);
            kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition)); //опрашиваем полностью партицию
            //без этой строчки выводит только последние записи

            for (ConsumerRecord<String, BaseDocument> record : records)
                System.out.println(record.value().getAttribute("firstName") + " " + record.value().getAttribute("lastName") +
                        " " + record.value().getAttribute("age") + " " + record.value().getAttribute("profession"));
        }
    }
}

