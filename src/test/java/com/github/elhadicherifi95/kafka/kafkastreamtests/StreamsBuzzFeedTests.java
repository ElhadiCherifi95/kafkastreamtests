package com.github.elhadicherifi95.kafka.kafkastreamtests;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

public class StreamsBuzzFeedTests {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private Serde<String> stringSerde = new Serdes.StringSerde();
    private Serde<Long> longSerde = new Serdes.LongSerde();

    @Before
    public void setTopologyTestDriver(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuzzFeed streamsBuzzFeed = new StreamsBuzzFeed();
        Topology topology = streamsBuzzFeed.createTopology();
        testDriver = new TopologyTestDriver(topology, props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), stringSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }


    @Test
    public void dummuyTest(){
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }
}
