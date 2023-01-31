package kafka;

import com.arangodb.entity.BaseDocument;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class SampleProducer<T> {
    public SampleProducer(T value) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",  "org.springframework.kafka.support.serializer.JsonSerializer");


        ProducerRecord producerRecord = new ProducerRecord<>("test", Integer.toString(value.hashCode()), value);
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.send(producerRecord);
        kafkaProducer.close();

    }

}
