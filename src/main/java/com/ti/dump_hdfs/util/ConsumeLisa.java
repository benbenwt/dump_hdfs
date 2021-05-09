package com.ti.dump_hdfs.util;


import com.ti.dump_hdfs.Insert_stix;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class ConsumeLisa {
    public static String getCurrentDate()
    {
        Date date=new Date();
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
        String format_date=simpleDateFormat.format(date);
        return  format_date;
    }
    public  void consume(String url,String topic) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topic));
        Insert_stix insert_stix=new Insert_stix();
        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                insert_stix.submitStixDirectory(record.value(),"hdfs://hbase:9000/user/root/stix/"+getCurrentDate()+"/");
            }
        }
        consumer.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        new ConsumeLisa().consume("hbase:9092","lisa");
    }
}
