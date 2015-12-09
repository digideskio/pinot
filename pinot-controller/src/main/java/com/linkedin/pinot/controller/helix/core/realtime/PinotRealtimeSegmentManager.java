/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.helix.core.realtime;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Realtime segment manager, which assigns realtime segments to server instances so that they can consume from Kafka.
 */
public class PinotRealtimeSegmentManager implements HelixPropertyListener, IZkChildListener, IZkDataListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeSegmentManager.class);
  private static final String TABLE_CONFIG = "/CONFIGS/TABLE";
  private static final String SEGMENTS_PATH = "/SEGMENTS";
  private static final String REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN =
      ".*/SEGMENTS/.*_REALTIME|.*/SEGMENTS/.*_REALTIME/.*";

  private String _propertyStorePath;
  private String _tableConfigPath;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private ZkClient _zkClient;



  public PinotRealtimeSegmentManager(PinotHelixResourceManager pinotManager) {
    _pinotHelixResourceManager = pinotManager;
    String clusterName = _pinotHelixResourceManager.getHelixClusterName();
    _propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    _tableConfigPath = _propertyStorePath + TABLE_CONFIG;
  }

  public void start() {
    LOGGER.info("Starting realtime segments manager, adding a listener on the property store table configs path.");
    String zkUrl = _pinotHelixResourceManager.getHelixZkURL();
    _zkClient = new ZkClient(zkUrl, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    _zkClient.waitUntilConnected();

    // Subscribe to any data/child changes to property
    _zkClient.subscribeChildChanges(_tableConfigPath, this);
    _zkClient.subscribeDataChanges(_tableConfigPath, this);

    // Setup change listeners for already existing tables, if any.
    processPropertyStoreChange(_tableConfigPath);
  }

  public void stop() {
    LOGGER.info("Stopping realtime segments manager, stopping property store.");
    _pinotHelixResourceManager.getPropertyStore().stop();
  }

  private synchronized void assignRealtimeSegmentsToServerInstancesIfNecessary()
      throws JSONException, IOException {
    // Fetch current ideal state snapshot
    Map<String, IdealState> idealStateMap = new HashMap<String, IdealState>();

    for (String resource : _pinotHelixResourceManager.getAllRealtimeTables()) {
      idealStateMap.put(resource, _pinotHelixResourceManager.getHelixAdmin()
          .getResourceIdealState(_pinotHelixResourceManager.getHelixClusterName(), resource));
    }

    List<String> listOfSegmentsToAdd = new ArrayList<String>();

    for (String resource : idealStateMap.keySet()) {
      IdealState state = idealStateMap.get(resource);

      // Are there any partitions?
      if (state.getPartitionSet().size() == 0) {
        // No, this is a brand new ideal state, so we will add one new segment to every partition and replica
        List<String> instancesInResource = new ArrayList<String>();
        try {
          instancesInResource
              .addAll(_pinotHelixResourceManager.getServerInstancesForTable(resource, TableType.REALTIME));
        } catch (Exception e) {
          LOGGER.error("Caught exception while fetching instances for resource {}", resource, e);
        }

        // Assign a new segment to all server instances
        for (String instanceId : instancesInResource) {
          InstanceZKMetadata instanceZKMetadata = _pinotHelixResourceManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime
              .build(resource, instanceId, groupId, partitionId, String.valueOf(System.currentTimeMillis())));
        }
      } else {
        // Add all server instances to the list of instances for which to assign a realtime segment
        Set<String> instancesToAssignRealtimeSegment = new HashSet<String>();
        instancesToAssignRealtimeSegment
            .addAll(_pinotHelixResourceManager.getServerInstancesForTable(resource, TableType.REALTIME));

        // Remove server instances that are currently processing a segment
        for (String partition : state.getPartitionSet()) {
          RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
              ZKMetadataProvider.getRealtimeSegmentZKMetadata(_pinotHelixResourceManager.getPropertyStore(),
                  SegmentNameBuilder.Realtime.extractTableName(partition), partition);
          if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
            String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(partition);
            instancesToAssignRealtimeSegment.remove(instanceName);
          }
        }

        // Assign a new segment to the server instances not currently processing this segment
        for (String instanceId : instancesToAssignRealtimeSegment) {
          InstanceZKMetadata instanceZKMetadata = _pinotHelixResourceManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime
              .build(resource, instanceId, groupId, partitionId, String.valueOf(System.currentTimeMillis())));
        }
      }
    }

    LOGGER.info("Computed list of new segments to add : " + Arrays.toString(listOfSegmentsToAdd.toArray()));

    // Add the new segments to the server instances
    for (String segmentId : listOfSegmentsToAdd) {
      String resourceName = SegmentNameBuilder.Realtime.extractTableName(segmentId);
      String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(segmentId);

      // Does the ideal state already contain this segment?
      if (!idealStateMap.get(resourceName).getPartitionSet().contains(segmentId)) {
        // No, add it
        // Create the realtime segment metadata
        RealtimeSegmentZKMetadata realtimeSegmentMetadataToAdd = new RealtimeSegmentZKMetadata();
        realtimeSegmentMetadataToAdd.setTableName(TableNameBuilder.extractRawTableName(resourceName));
        realtimeSegmentMetadataToAdd.setSegmentType(SegmentType.REALTIME);
        realtimeSegmentMetadataToAdd.setStatus(Status.IN_PROGRESS);
        realtimeSegmentMetadataToAdd.setSegmentName(segmentId);

        // Add the new metadata to the property store
        ZKMetadataProvider
            .setRealtimeSegmentZKMetadata(_pinotHelixResourceManager.getPropertyStore(), realtimeSegmentMetadataToAdd);

        // Update the ideal state to add the new realtime segment
        HelixHelper.updateIdealState(_pinotHelixResourceManager.getHelixZkManager(), resourceName,
            idealState -> PinotTableIdealStateBuilder
                .addNewRealtimeSegmentToIdealState(segmentId, idealState, instanceName),
            RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
      }
    }
  }

  private boolean isLeader() {
    return _pinotHelixResourceManager.isLeader();
  }

  @Override
  public synchronized void onDataChange(String path) {
    processPropertyStoreChange(path);
  }

  @Override
  public synchronized void onDataCreate(String path) {
    processPropertyStoreChange(path);
  }

  @Override
  public synchronized void onDataDelete(String path) {
    processPropertyStoreChange(path);
  }

  private void processPropertyStoreChange(String path) {
    try {
      LOGGER.info("Processing change notification for path:{}", path);
      refreshWatchers(path);

      if (isLeader()) {
        if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
          assignRealtimeSegmentsToServerInstancesIfNecessary();
        }
      } else {
        LOGGER.info("Not the leader of this cluster, ignoring realtime segment property store change.");
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing change for path {}", path, e);
      Utils.rethrowException(e);
    }
  }

  /**
   * Helper method to perform idempotent operation to refresh all watches (related to real-time segments):
   * - Data change listener for all existing real-time tables.
   * - Child creation listener for all existing real-time tables.
   * - Data change listener for all existing real-time segments
   *
   * @param path
   */
  private void refreshWatchers(String path) {
    LOGGER.info("Received change notification for path: {}", path);
    List<Stat> stats = new ArrayList<>();
    List<ZNRecord> tableConfigs = _pinotHelixResourceManager.getPropertyStore().getChildren(TABLE_CONFIG, stats, 0);

    if (tableConfigs == null) {
      return;
    }

    for (ZNRecord tableConfig : tableConfigs) {
      try {
        AbstractTableConfig abstractTableConfig = AbstractTableConfig.fromZnRecord(tableConfig);
        if (abstractTableConfig.isRealTime()) {
          String realtimeTable = abstractTableConfig.getTableName();
          String realtimeSegmentsPathForTable = _propertyStorePath + SEGMENTS_PATH + "/" + realtimeTable;

          LOGGER.info("Setting data/child changes watch for real-time table '{}'", realtimeTable);
          _zkClient.subscribeDataChanges(realtimeSegmentsPathForTable, this);
          _zkClient.subscribeChildChanges(realtimeSegmentsPathForTable, this);

          List<String> childNames =
              _pinotHelixResourceManager.getPropertyStore().getChildNames(SEGMENTS_PATH + "/" + realtimeTable, 0);

          if (childNames != null && !childNames.isEmpty()) {
            for (String segmentName : childNames) {
              String segmentPath = realtimeSegmentsPathForTable + "/" + segmentName;
              LOGGER.info("Setting data change watch for real-time segment: {}", segmentPath);
              _zkClient.subscribeDataChanges(segmentPath, this);
            }
          }
        }
      } catch (JSONException e) {
        LOGGER.error("Caught exception while reading table config", e);
      } catch (IOException e) {
        LOGGER.error("Caught exception while setting change listeners for realtime tables/segments", e);
      }
    }
  }

  @Override
  public void handleChildChange(String parentPath, List<String> currentChilds)
      throws Exception {
    processPropertyStoreChange(parentPath);
  }

  @Override
  public void handleDataChange(String dataPath, Object data)
      throws Exception {
    processPropertyStoreChange(dataPath);
  }

  @Override
  public void handleDataDeleted(String dataPath)
      throws Exception {
    processPropertyStoreChange(dataPath);
  }
}
