package com.github.elhadicherifi95.kafka.kafkastreamtests;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class StreamsBuzzFeedTests {

    TopologyTestDriver testDriver;
    private TestInputTopic<String, String> buzzFeedTopic;
    private TestOutputTopic<String, String> buzzFeedPredicateTopic;
    private TestOutputTopic<String, String> buzzFeedMapperTopic;
    private TestOutputTopic<String, Long> buzzFeedCounterTopic;

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
        buzzFeedTopic = testDriver.createInputTopic("buzz-feed", stringSerde.serializer(), stringSerde.serializer());
        buzzFeedPredicateTopic = testDriver.createOutputTopic("buzz-feed-predicate", stringSerde.deserializer(), stringSerde.deserializer());
        buzzFeedMapperTopic = testDriver.createOutputTopic("buzz-feed-mapper", stringSerde.deserializer(), stringSerde.deserializer());
        buzzFeedCounterTopic = testDriver.createOutputTopic("buzz-feed-counter", stringSerde.deserializer(), longSerde.deserializer());

    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }


    @Test
    public void predicateTestor(){
        buzzFeedTopic.pipeInput("key", "1");
        buzzFeedTopic.pipeInput("key", "abcd");
        assertThat(buzzFeedPredicateTopic.readKeyValue(), equalTo(new KeyValue<>("key", "1")));
        assertTrue(buzzFeedPredicateTopic.isEmpty());
    }

    @Test
    public void mapperTestor(){
        buzzFeedTopic.pipeInput("key", "1");
        buzzFeedTopic.pipeInput("key", "3");
        buzzFeedTopic.pipeInput("key", "5");
        assertThat(buzzFeedMapperTopic.readKeyValue(), equalTo(new KeyValue<>("key", "buzzfeed")));
        assertThat(buzzFeedMapperTopic.readKeyValue(), equalTo(new KeyValue<>("key", "buzz")));
        assertThat(buzzFeedMapperTopic.readKeyValue(), equalTo(new KeyValue<>("key", "feed")));
        assertTrue(buzzFeedMapperTopic.isEmpty());
    }

    @Test
    public void counterTestor(){
        buzzFeedTopic.pipeInput("key", "1");
        buzzFeedTopic.pipeInput("key", "9");
        buzzFeedTopic.pipeInput("key", "3");
        buzzFeedTopic.pipeInput("key", "5");
        buzzFeedTopic.pipeInput("key", "20");
        buzzFeedTopic.pipeInput("key", "3");
        buzzFeedTopic.pipeInput("key", "1");


        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("buzzfeed", 1L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("buzz", 1L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("buzz", 2L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("feed", 1L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("feed", 2L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("buzz", 3L)));
        assertThat(buzzFeedCounterTopic.readKeyValue(), equalTo(new KeyValue<>("buzzfeed", 2L)));
        assertTrue(buzzFeedCounterTopic.isEmpty());

    }
}
