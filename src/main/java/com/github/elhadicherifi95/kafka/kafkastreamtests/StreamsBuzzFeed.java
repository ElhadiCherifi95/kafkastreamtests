package com.github.elhadicherifi95.kafka.kafkastreamtests;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class StreamsBuzzFeed {

    public Topology createTopology(){
        //Create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String,String> inputTopic = streamsBuilder.stream("buzz-feed");
        KStream<String,String> filtredStream = inputTopic.filter((k,v) -> isInteger(v));
        filtredStream.to("buzz-feed-predicate");
        KStream<String,String> mapperStream = filtredStream.mapValues(v -> buzzFeedProcessor(v));
        mapperStream.to("buzz-feed-mapper");
        KStream<String,Long> counterStream = mapperStream
                                                .groupBy((k,v)->  v)
                                                .count()
                                                .toStream();
        counterStream.to("buzz-feed-counter", Produced.with(Serdes.String(), Serdes.Long()));


        return streamsBuilder.build();
    }

    public static void main(String[] args) {
        //Create properties
        Properties properties = new Properties();
        String bootstrapServers ="127.0.0.1:9092";
        String id_config =  "demo-kafka-streams";
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,id_config);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //Build the topology
        StreamsBuzzFeed streamsBuzzFeed = new StreamsBuzzFeed();

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuzzFeed.createTopology(),properties);
        //Start our stream app
        kafkaStreams.start();
    }


    private static Boolean isInteger(String value){
        try{
            Integer.parseInt(value);
        }
        catch (NumberFormatException e){
            return false;
        }

        return true;
    }

    private static String buzzFeedProcessor(String value){

        Integer intValue = Integer.parseInt(value);
        if (intValue%3 ==0)
            return "buzz";
        else if (intValue%5 ==0)
            return "feed";
        return "buzzfeed";
    }
}
