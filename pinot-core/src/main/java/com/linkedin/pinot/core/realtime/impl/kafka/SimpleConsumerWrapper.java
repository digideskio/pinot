/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.common.utils.ZkStarter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TODO Document me!
 */
public class SimpleConsumerWrapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerWrapper.class);

  private enum ConsumerState {
    CONNECTING_TO_BOOTSTRAP_NODE,
    CONNECTED_TO_BOOTSTRAP_NODE,
    FETCHING_LEADER_INFORMATION,
    CONNECTING_TO_PARTITION_LEADER,
    CONNECTED_TO_PARTITION_LEADER
  }

  private State _currentState;

  private final boolean _metadataOnlyConsumer;
  private final String _topic;
  private final int _partition;
  private final String _bootstrapNodes;
  private SimpleConsumer _simpleConsumer;

  private SimpleConsumerWrapper(String bootstrapNodes) {
    _bootstrapNodes = bootstrapNodes;
    _metadataOnlyConsumer = true;
    _simpleConsumer = null;

    // Topic and partition are ignored for metadata-only consumers
    _topic = null;
    _partition = Integer.MIN_VALUE;

    setCurrentState(new ConnectingToBootstrapNode());
  }

  private SimpleConsumerWrapper(String bootstrapNodes, String topic, int partition) {
    _bootstrapNodes = bootstrapNodes;
    _topic = topic;
    _partition = partition;
    _metadataOnlyConsumer = false;
    _simpleConsumer = null;

    setCurrentState(new ConnectingToBootstrapNode());
  }

  private abstract class State {
    private ConsumerState stateValue;

    protected State(ConsumerState stateValue) {
      this.stateValue = stateValue;
    }

    abstract void think();

    abstract boolean isConnectedToKafkaBroker();

    void handleConsumerException(Exception e) {
      // TODO Remove this
      System.out.println("e = " + e);
      e.printStackTrace();

      // By default, just log the exception and switch back to CONNECTING_TO_BOOTSTRAP_NODE (which will take care of
      // closing the connection if it exists)
      LOGGER.warn("Caught Kafka consumer exception, disconnecting and trying again", e);

      setCurrentState(new ConnectingToBootstrapNode());
    }

    ConsumerState getStateValue() {
      return stateValue;
    }
  }

  private class ConnectingToBootstrapNode extends State {
    public ConnectingToBootstrapNode() {
      super(ConsumerState.CONNECTING_TO_BOOTSTRAP_NODE);
    }

    @Override
    public void think() {
      // Connect to a random bootstrap node
      if (_simpleConsumer != null) {
        try {
          _simpleConsumer.close();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while closing consumer, ignoring", e);
        }
      }

      // TODO Parse bootstrapNodes
      // TODO Remove magic numbers
      try {
        _simpleConsumer = new SimpleConsumer("localhost", KafkaStarterUtils.DEFAULT_KAFKA_PORT, 10000, 10000, "potato");
        setCurrentState(new ConnectedToBootstrapNode());
      } catch (Exception e) {
        handleConsumerException(e);
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToBootstrapNode extends State {
    protected ConnectedToBootstrapNode() {
      super(ConsumerState.CONNECTED_TO_BOOTSTRAP_NODE);
    }

    @Override
    void think() {
      if (_metadataOnlyConsumer) {
        // Nothing to do
      } else {
        // If we're consuming from a partition, we need to find the leader so that we can consume from it. By design,
        // Kafka only allows consumption from the leader and not one of the in-sync replicas.
        setCurrentState(new FetchingLeaderInformation());
      }
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class FetchingLeaderInformation extends State {
    public FetchingLeaderInformation() {
      super(ConsumerState.FETCHING_LEADER_INFORMATION);
    }

    @Override
    void think() {
      // Fetch leader information
      // TODO Implement, for now, since it's a test, we're connected to the leader by definition
      setCurrentState(new ConnectingToPartitionLeader());
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private class ConnectingToPartitionLeader extends State {
    public ConnectingToPartitionLeader() {
      super(ConsumerState.CONNECTING_TO_PARTITION_LEADER);
    }

    @Override
    void think() {
      // Connect to the partition leader
      // TODO Implement, for now, since it's a test, we're connected to the leader by definition
      setCurrentState(new ConnectedToPartitionLeader());
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return false;
    }
  }

  private class ConnectedToPartitionLeader extends State {
    public ConnectedToPartitionLeader() {
      super(ConsumerState.CONNECTED_TO_PARTITION_LEADER);
    }

    @Override
    void think() {
      // Nothing to do
    }

    @Override
    boolean isConnectedToKafkaBroker() {
      return true;
    }
  }

  private void setCurrentState(State newState) {
    if (_currentState != null) {
      // TODO Turn this into a logger statement
      System.out.println("Switching from state " + _currentState.getStateValue() + " to state " + newState.getStateValue());
    }

    _currentState = newState;
  }

  public int getPartitionCount(String topic) {
    int unknownTopicReplyCount = 0;
    final int MAX_UNKNOWN_TOPIC_REPLY_COUNT = 10;
    int kafkaErrorCount = 0;
    final int MAX_KAFKA_ERROR_COUNT = 10;

    while(true) {
      // Try to get into a state where we're connected to Kafka
      // TODO This needs a time limit
      while (!_currentState.isConnectedToKafkaBroker()) {
        _currentState.think();
      }

      // Send the metadata request to Kafka
      TopicMetadataResponse topicMetadataResponse = null;
      try {
        topicMetadataResponse = _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(topic)));
      } catch (Exception e) {
        _currentState.handleConsumerException(e);
        continue;
      }

      final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
      final short errorCode = topicMetadata.errorCode();

      if (errorCode == Errors.NONE.code()) {
        return topicMetadata.partitionsMetadata().size();
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else if (errorCode == Errors.INVALID_TOPIC_EXCEPTION.code()) {
        throw new RuntimeException("Invalid topic name " + topic);
      } else if (errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
        if (MAX_UNKNOWN_TOPIC_REPLY_COUNT < unknownTopicReplyCount) {
          throw new RuntimeException("Topic " + topic + " does not exist");
        } else {
          // Kafka topic creation can sometimes take some time, so we'll retry after a little bit
          unknownTopicReplyCount++;
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      } else {
        // Retry after a short delay
        kafkaErrorCount++;

        if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
          throw Errors.forCode(errorCode).exception();
        }

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }
  }

  public Iterable<MessageAndOffset> fetchMessages(long startOffset, long endOffset, int timeoutMillis) {
    // Ensure that we're connected to the leader
    // TODO Add a timeout/error handling
    while(_currentState.getStateValue() != ConsumerState.CONNECTED_TO_PARTITION_LEADER) {
      _currentState.think();
    }

    FetchResponse fetchResponse = _simpleConsumer.fetch(new FetchRequestBuilder()
        .minBytes(100000)
        .maxWait(timeoutMillis)
        .addFetch(_topic, _partition, startOffset, 500000)
        .build());

    return buildOffsetFilteringIterable(fetchResponse.messageSet(_topic, _partition), startOffset, endOffset);
  }

  private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets, final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, new Predicate<MessageAndOffset>() {
      @Override
      public boolean apply(@Nullable MessageAndOffset input) {
        // Filter messages that are either null or have an offset ∉ [startOffset; endOffset[
        if(input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
          return false;
        }

        // Check the message's checksum
        // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
        if(!input.message().isValid()) {
          LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
          return false;
        }

        return true;
      }
    });
  }

  /**
   * Creates a simple consumer wrapper that connects to a random Kafka broker, which allows for fetching topic and
   * partition metadata. It does not allow to consume from a partition, since Kafka requires connecting to the
   * leader of that partition for consumption.
   *
   * @param bootstrapNodes A comma separated list of Kafka broker nodes
   * @return A consumer wrapper
   */
  public static SimpleConsumerWrapper forMetadataConsumption(String bootstrapNodes) {
    return new SimpleConsumerWrapper(bootstrapNodes);
  }

  /**
   * Creates a simple consumer wrapper that automatically connects to the leader broker for the given topic and
   * partition. This consumer wrapper can also fetch topic and partition metadata.
   *
   * @param bootstrapNodes A comma separated list of Kafka broker nodes
   * @param topic The Kafka topic to consume from
   * @param partition The partition id to consume from
   * @return A consumer wrapper
   */
  public static SimpleConsumerWrapper forPartitionConsumption(String bootstrapNodes, String topic, int partition) {
    return new SimpleConsumerWrapper(bootstrapNodes, topic, partition);
  }

  public static void main(String[] args) {
    // TODO Remove this before merging
    try {
      ZkStarter.startLocalZkServer();
      KafkaStarterUtils.startServer(KafkaStarterUtils.DEFAULT_KAFKA_PORT, KafkaStarterUtils.DEFAULT_BROKER_ID, ZkStarter.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());
      KafkaStarterUtils.createTopic("potato", ZkStarter.DEFAULT_ZK_STR);
      SimpleConsumerWrapper consumerWrapper = forPartitionConsumption(KafkaStarterUtils.DEFAULT_KAFKA_BROKER, "potato", 0);
      System.out.println(consumerWrapper.getPartitionCount("potato"));
      System.out.println(consumerWrapper.fetchMessages(0, 10, 1000));
    } catch (Exception e) {
      System.out.println("e = " + e);
      e.printStackTrace();
    }

    System.exit(-1);
  }
}
