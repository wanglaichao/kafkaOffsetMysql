package test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by laichao.wang on 2018/12/20.
 */
public class ConsumerDemo extends Thread {

    private String topic;
    public ConsumerDemo(String topic){
        super();
        this.topic = topic;
    }
    @Override
    public void run() {
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1); // 1表示consumer thread线程数量
        Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
        ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
        while(iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("接收到: " + message);
        }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "10.0.5.163:9092,10.0.5.164:9092");//声明zk
        properties.put("zookeeper.connect", "10.0.5.164:2181/kafka");//声明zk
        properties.put("group.id", "test1_topic_debug");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
    }

    public static void main(String[] args) {
        String test = "test1_topic";
        new ConsumerDemo(test).start();// 使用kafka集群中创建好的主题 test
    }
}
