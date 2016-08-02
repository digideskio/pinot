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
package com.linkedin.pinot.core.segment.index.loader;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;
import com.linkedin.pinot.core.segment.creator.ForwardIndexType;
import com.linkedin.pinot.core.segment.creator.InvertedIndexType;
import com.linkedin.pinot.core.segment.creator.impl.SegmentColumnarIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import com.linkedin.pinot.core.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Use mmap to load the segment and perform all pre-processing steps. (This can be slow)
 * Pre-processing steps include:
 * - Generate inverted index.
 * - Generate default value for new added columns.
 */
public class SegmentPreProcessor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreProcessor.class);

  private final File indexDir;
  private SegmentMetadataImpl segmentMetadata;
  private final String segmentName;
  private final SegmentVersion segmentVersion;
  private final PropertiesConfiguration segmentProperties;
  private final IndexLoadingConfigMetadata indexConfig;
  private final Schema schema;
  private final SegmentDirectory segmentDirectory;

  private enum DefaultValueAction {
    // Present in schema but not in segment.
    ADD_DIMENSION,
    ADD_METRIC,
    // Present in schema & segment but default value doesn't match.
    UPDATE_DIMENSION,
    UPDATE_METRIC,
    // Present in segment but not in schema, auto-generated.
    REMOVE_DIMENSION,
    REMOVE_METRIC;

    boolean isRemoveAction() {
      return this == REMOVE_DIMENSION || this == REMOVE_METRIC;
    }
  }

  SegmentPreProcessor(File indexDir, IndexLoadingConfigMetadata indexConfig, Schema schema)
      throws Exception {
    Preconditions.checkNotNull(indexDir);
    Preconditions.checkState(indexDir.exists(), "Segment directory: {} does not exist", indexDir);
    Preconditions.checkState(indexDir.isDirectory(), "Segment path: {} is not a directory", indexDir);

    this.indexDir = indexDir;
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    segmentName = segmentMetadata.getName();
    segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());
    segmentProperties = segmentMetadata.getSegmentMetadataPropertiesConfiguration();
    this.indexConfig = indexConfig;
    this.schema = schema;
    // Always use mmap to load the segment because it is safest and performs well without impact from -Xmx params.
    // This is not the final load of the segment.
    segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
  }

  public void process()
      throws Exception {
    SegmentDirectory.Writer segmentWriter = null;
    try {
      segmentWriter = segmentDirectory.createWriter();
      createInvertedIndices(segmentWriter);

      // This step may modify the segment metadata. When adding new steps after this, reload the segment metadata.
      updateDefaultValueForNewColumns(segmentWriter);
    } finally {
      if (segmentWriter != null) {
        segmentWriter.saveAndClose();
      }
    }
  }

  private void createInvertedIndices(SegmentDirectory.Writer segmentWriter)
      throws IOException {
    Set<String> invertedIndexColumns = getInvertedIndexColumns();

    for (String column : invertedIndexColumns) {
      createInvertedIndexForColumn(segmentWriter, segmentMetadata.getColumnMetadataFor(column));
    }
  }

  private Set<String> getInvertedIndexColumns() {
    Set<String> invertedIndexColumns = new HashSet<>();
    if (indexConfig == null) {
      return invertedIndexColumns;
    }

    Set<String> invertedIndexColumnsFromConfig = indexConfig.getLoadingInvertedIndexColumns();
    for (String column : invertedIndexColumnsFromConfig) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.isSorted()) {
        invertedIndexColumns.add(column);
      }
    }

    return invertedIndexColumns;
  }

  private void createInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws IOException {
    String column = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, column + ".inv.inprogress");
    File invertedIndexFile = new File(indexDir, column + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (segmentWriter.hasIndexFor(column, ColumnIndexType.INVERTED_INDEX)) {
        // Skip creating inverted index if already exists.

        LOGGER.info("Found inverted index for segment: {}, column: {}", segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove inverted index if exists.
      // For v1 and v2, it's the actual inverted index. For v3, it's the temporary inverted index.
      FileUtils.deleteQuietly(invertedIndexFile);
    }

    // Create new inverted index for the column.
    LOGGER.info("Creating new inverted index for segment: {}, column: {}", segmentName, column);
    int totalDocs = columnMetadata.getTotalDocs();
    OffHeapBitmapInvertedIndexCreator creator =
        new OffHeapBitmapInvertedIndexCreator(indexDir, columnMetadata.getCardinality(), totalDocs,
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.toFieldSpec());

    try (DataFileReader fwdIndex = getForwardIndexReader(columnMetadata, segmentWriter)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.

        FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;
        for (int i = 0; i < totalDocs; i++) {
          creator.add(i, svFwdIndex.getInt(i));
        }
      } else {
        // Multi-value column.

        SingleColumnMultiValueReader mvFwdIndex = (SingleColumnMultiValueReader) fwdIndex;
        int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
        for (int i = 0; i < totalDocs; i++) {
          int len = mvFwdIndex.getIntArray(i, dictIds);
          creator.add(i, dictIds, len);
        }
      }
    }

    creator.seal();

    // For v3, write the generated inverted index file into the single file and remove it.
    if (segmentVersion == SegmentVersion.v3) {
      writeIndexToV3Format(segmentWriter, column, invertedIndexFile, ColumnIndexType.INVERTED_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created inverted index for segment: {}, column: {}", segmentName, column);
  }

  private DataFileReader getForwardIndexReader(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    if (columnMetadata.isSingleValue()) {
      return new FixedBitSingleValueReader(buffer, columnMetadata.getTotalDocs(), columnMetadata.getBitsPerElement(),
          columnMetadata.hasNulls());
    } else {
      return new FixedBitMultiValueReader(buffer, columnMetadata.getTotalDocs(),
          columnMetadata.getTotalNumberOfEntries(), columnMetadata.getBitsPerElement(), false);
    }
  }

  /**
   * Helper method to write an index file to v3 format single file and remove it.
   *
   * @param segmentWriter v3 format segment writer.
   * @param column column name.
   * @param indexFile index file to write from.
   * @param indexType index type.
   * @throws IOException
   */
  private static void writeIndexToV3Format(SegmentDirectory.Writer segmentWriter, String column, File indexFile,
      ColumnIndexType indexType)
      throws IOException {
    int fileLength = (int) indexFile.length();
    PinotDataBuffer buffer = null;
    try {
      if (segmentWriter.hasIndexFor(column, indexType)) {
        // Index already exists, try to reuse it.

        buffer = segmentWriter.getIndexFor(column, indexType);
        if (buffer.size() != fileLength) {
          // Existed index size is not equal to index file size.
          // Throw exception to drop and re-download the segment.

          String failureMessage =
              "V3 format segment already has " + indexType + " for column: " + column + " that cannot be removed.";
          LOGGER.error(failureMessage);
          throw new IllegalStateException(failureMessage);
        }
      } else {
        // Index does not exist, create a new buffer for that.

        buffer = segmentWriter.newIndexFor(column, indexType, fileLength);
      }

      buffer.readFrom(indexFile);
    } finally {
      FileUtils.deleteQuietly(indexFile);
      if (buffer != null) {
        buffer.close();
      }
    }
  }

  private void updateDefaultValueForNewColumns(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    if (schema == null) {
      return;
    }

    // Compute the action needed for each column.
    Map<String, DefaultValueAction> updateDefaultValueActionMap = computeUpdateDefaultValueActionMap();
    if (updateDefaultValueActionMap.isEmpty()) {
      return;
    }

    // Create the dictionary and forward index in temporary directory for ADD and UPDATE action columns.
    for (Map.Entry<String, DefaultValueAction> entry : updateDefaultValueActionMap.entrySet()) {
      // This method updates the metadata properties, need to save it later.
      updateDefaultValueForColumn(segmentWriter, entry.getKey(), entry.getValue());
    }

    // Update the segment metadata.
    List<String> dimensionColumns = getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.DIMENSIONS);
    List<String> metricColumns = getStringListFromSegmentProperties(V1Constants.MetadataKeys.Segment.METRICS);
    for (Map.Entry<String, DefaultValueAction> entry : updateDefaultValueActionMap.entrySet()) {
      String column = entry.getKey();
      DefaultValueAction action = entry.getValue();
      switch (action) {
        case ADD_DIMENSION:
          dimensionColumns.add(column);
          break;
        case ADD_METRIC:
          metricColumns.add(column);
          break;
        case REMOVE_DIMENSION:
          dimensionColumns.remove(column);
          break;
        case REMOVE_METRIC:
          metricColumns.remove(column);
          break;
        default:
          break;
      }
    }
    segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.DIMENSIONS, dimensionColumns);
    segmentProperties.setProperty(V1Constants.MetadataKeys.Segment.METRICS, metricColumns);

    // Create a back up for origin metadata.
    File metadataFile = new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME);
    File metadataBackUpFile = new File(metadataFile + ".bak");
    if (!metadataBackUpFile.exists()) {
      FileUtils.copyFile(metadataFile, metadataBackUpFile);
    }

    // Save the new metadata.
    segmentProperties.save(metadataFile);
  }

  /**
   * Compute the action needed for each column.
   * This method compares the column metadata across schema and segment.
   *
   * @return Action Map for each column.
   */
  private Map<String, DefaultValueAction> computeUpdateDefaultValueActionMap() {
    Map<String, DefaultValueAction> updateDefaultValueActionMap = new HashMap<>();

    // Compute ADD and UPDATE actions.
    Collection<String> columnsInSchema = schema.getColumnNames();
    for (String column : columnsInSchema) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

      if (columnMetadata != null) {
        // Column exists in the segment, check if we need to update the value.

        // Only check for auto-generated column.
        if (columnMetadata.isAutoGenerated()) {

          // Check the field type matches.
          FieldSpec.FieldType fieldTypeFromMetadata = columnMetadata.getFieldType();
          if (fieldTypeFromMetadata != fieldType) {
            String failureMessage = "Field type: " + fieldTypeFromMetadata + " for auto-generated column: " + column
                + " does not match field type: " + fieldType
                + " in schema, throw exception to drop and re-download the segment.";
            LOGGER.error(failureMessage);
            throw new RuntimeException(failureMessage);
          }

          // Check the data type and default value matches.
          FieldSpec.DataType dataTypeFromMetadata = columnMetadata.getDataType();
          FieldSpec.DataType dataTypeFromSchema = fieldSpec.getDataType();
          boolean isSingleValueInMetadata = columnMetadata.isSingleValue();
          boolean isSingleValueInSchema = fieldSpec.isSingleValueField();
          String defaultValueFromMetadata = columnMetadata.getDefaultNullValueString();
          String defaultValueFromSchema = fieldSpec.getDefaultNullValue().toString();
          if (dataTypeFromMetadata != dataTypeFromSchema || isSingleValueInMetadata != isSingleValueInSchema
              || !defaultValueFromSchema.equals(defaultValueFromMetadata)) {
            switch (fieldType) {
              case DIMENSION:
                updateDefaultValueActionMap.put(column, DefaultValueAction.UPDATE_DIMENSION);
                break;
              case METRIC:
                updateDefaultValueActionMap.put(column, DefaultValueAction.UPDATE_METRIC);
                break;
              default:
                throw new UnsupportedOperationException(
                    "Updating " + fieldType + " column is not supported in schema evolution");
            }
          }
        }
      } else {
        // Column does not exist in the segment, add default value for it.

        switch (fieldType) {
          case DIMENSION:
            updateDefaultValueActionMap.put(column, DefaultValueAction.ADD_DIMENSION);
            break;
          case METRIC:
            updateDefaultValueActionMap.put(column, DefaultValueAction.ADD_METRIC);
            break;
          default:
            throw new UnsupportedOperationException(
                "Adding " + fieldType + " column is not supported in schema evolution");
        }
      }
    }

    // Compute REMOVE actions.
    Set<String> columnsInSegment = segmentMetadata.getAllColumns();
    for (String column : columnsInSegment) {
      if (!columnsInSchema.contains(column)) {
        ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);

        // Only remove auto-generated columns.
        if (columnMetadata.isAutoGenerated()) {
          FieldSpec.FieldType fieldType = columnMetadata.getFieldType();
          switch (fieldType) {
            case DIMENSION:
              updateDefaultValueActionMap.put(column, DefaultValueAction.REMOVE_DIMENSION);
              break;
            case METRIC:
              updateDefaultValueActionMap.put(column, DefaultValueAction.REMOVE_METRIC);
              break;
            default:
              throw new UnsupportedOperationException(
                  "Removing " + fieldType + " column is not supported in schema evolution");
          }
        }
      }
    }

    return updateDefaultValueActionMap;
  }

  /**
   * Helper method to update default value for segment column indices.
   * TODO: ADD SUPPORT TO STAR TREE INDEX.
   *
   * @param segmentWriter segment writer.
   * @param column column name.
   * @param action default value action.
   * @throws Exception
   */
  private void updateDefaultValueForColumn(SegmentDirectory.Writer segmentWriter, String column,
      DefaultValueAction action)
      throws Exception {
    LOGGER.info("Starting default value action: {} on column: {}", action, column);

    // Column indices cannot be removed for segment format v3.
    // Throw exception to drop and re-download the segment.
    if (action.isRemoveAction() && segmentVersion == SegmentVersion.v3) {
      String failureMessage =
          "Default value indices for column: " + column + " cannot be removed for segment format v3, throw exception to"
              + " drop and re-download the segment.";
      LOGGER.error(failureMessage);
      throw new RuntimeException(failureMessage);
    }

    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    boolean isSingleValue = fieldSpec.isSingleValueField();

    // Delete existing dictionary and forward index for the new column.
    // NOTE: for ADD and version v3, this is for error-handling.
    File dictionaryFile = new File(indexDir, column + V1Constants.Dict.FILE_EXTENTION);
    File forwardIndexFile;
    if (isSingleValue) {
      forwardIndexFile = new File(indexDir, column + V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION);
    } else {
      forwardIndexFile = new File(indexDir, column + V1Constants.Indexes.UN_SORTED_MV_FWD_IDX_FILE_EXTENTION);
    }
    FileUtils.deleteQuietly(dictionaryFile);
    FileUtils.deleteQuietly(forwardIndexFile);

    // Remove the column metadata information if exists.
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(segmentProperties, column);

    // Now we finished all the work needed for REMOVE action.

    if (!action.isRemoveAction()) {
      // For ADD and UPDATE action, need to generate new dictionary and forward index, and update column metadata.

      // Generate column index creation information.
      int totalDocs = segmentMetadata.getTotalDocs();
      int totalRawDocs = segmentMetadata.getTotalRawDocs();
      int totalAggDocs = totalDocs - totalRawDocs;
      Object defaultValue = fieldSpec.getDefaultNullValue();
      String stringDefaultValue = defaultValue.toString();
      FieldSpec.DataType dataType = fieldSpec.getDataType();
      int maxNumberOfMultiValueElements = isSingleValue ? 0 : 1;
      // Only useful for String type.
      int dictionaryElementSize = 0;
      Object sortedArray;

      switch (dataType) {
        case BOOLEAN:
        case STRING:
          byte[] bytes = stringDefaultValue.getBytes("UTF8");
          dictionaryElementSize = bytes.length;
          sortedArray = new String[]{stringDefaultValue};
          break;
        // NOTE: No support for SHORT in dictionary,
        case SHORT:
          sortedArray = new short[]{Short.valueOf(stringDefaultValue)};
          break;
        case INT:
          sortedArray = new int[]{Integer.valueOf(stringDefaultValue)};
          break;
        case LONG:
          sortedArray = new long[]{Long.valueOf(stringDefaultValue)};
          break;
        case FLOAT:
          sortedArray = new float[]{Float.valueOf(stringDefaultValue)};
          break;
        case DOUBLE:
          sortedArray = new double[]{Double.valueOf(stringDefaultValue)};
          break;
        default:
          throw new UnsupportedOperationException(
              "Schema evolution not supported for data type:" + fieldSpec.getDataType());
      }
      ColumnIndexCreationInfo columnIndexCreationInfo =
          new ColumnIndexCreationInfo(true/*createDictionary*/, defaultValue/*min*/, defaultValue/*max*/, sortedArray,
              ForwardIndexType.FIXED_BIT_COMPRESSED, InvertedIndexType.SORTED_INDEX, isSingleValue/*isSortedColumn*/,
              false/*hasNulls*/, totalDocs/*totalNumberOfEntries*/, maxNumberOfMultiValueElements,
              true/*isAutoGenerated*/, defaultValue/*defaultNullValue*/);

      // Create dictionary.
      // We will have only one value in the dictionary.
      SegmentDictionaryCreator segmentDictionaryCreator =
          new SegmentDictionaryCreator(false/*hasNulls*/, sortedArray, fieldSpec, indexDir,
              V1Constants.Str.DEFAULT_STRING_PAD_CHAR);
      segmentDictionaryCreator.build(new boolean[]{true}/*isSorted*/);
      segmentDictionaryCreator.close();

      // Create forward index.
      if (isSingleValue) {
        // Single-value column.

        SingleValueSortedForwardIndexCreator svFwdIndexCreator =
            new SingleValueSortedForwardIndexCreator(indexDir, 1/*cardinality*/, fieldSpec);
        for (int docId = 0; docId < totalDocs; docId++) {
          svFwdIndexCreator.add(0/*dictionaryId*/, docId);
        }
        svFwdIndexCreator.close();
      } else {
        // Multi-value column.

        MultiValueUnsortedForwardIndexCreator mvFwdIndexCreator =
            new MultiValueUnsortedForwardIndexCreator(fieldSpec, indexDir, 1/*cardinality*/, totalDocs/*numDocs*/,
                totalDocs/*totalNumberOfValues*/, false/*hasNulls*/);
        int[] dictionaryIds = {0};
        for (int docId = 0; docId < totalDocs; docId++) {
          mvFwdIndexCreator.index(docId, dictionaryIds);
        }
        mvFwdIndexCreator.close();
      }

      // For v3, write the generated dictionary and forward index file into the single file and remove them.
      if (segmentVersion == SegmentVersion.v3) {
        writeIndexToV3Format(segmentWriter, column, dictionaryFile, ColumnIndexType.DICTIONARY);
        writeIndexToV3Format(segmentWriter, column, forwardIndexFile, ColumnIndexType.FORWARD_INDEX);
      }

      // Update the metadata properties for this column.
      SegmentColumnarIndexCreator.addColumnMetadataInfo(segmentProperties, column, columnIndexCreationInfo, totalDocs,
          totalRawDocs, totalAggDocs, fieldSpec, dictionaryElementSize, true/*hasInvertedIndex*/);
    }
  }

  /**
   * Helper method to get string list from segment properties.
   *
   * @param key property key.
   * @return string list value for the property.
   */
  private List<String> getStringListFromSegmentProperties(String key) {
    List<String> stringList = new ArrayList<>();
    List propertyList = segmentProperties.getList(key);
    if (propertyList != null) {
      for (Object value : propertyList) {
        String stringValue = String.valueOf(value);
        if (!stringValue.isEmpty()) {
          stringList.add(stringValue);
        }
      }
    }
    return stringList;
  }

  @Override
  public void close()
      throws Exception {
    segmentDirectory.close();
  }
}
