/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.mapred.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.CopyListingFileStatus;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

/**
 * DynamicInputFormat implements the "Worker pattern" for DistCp.
 * DynamicInputFormat 实现 Work 的模式
 * Rather than to split up the copy-list into a set of static splits,
 * 将 copylist 再进行一次静态切分
 * the DynamicInputFormat does the following:
 * 1. Splits the copy-list into small chunks on the DFS.
 *  将 copy-list  切分为更小块
 * 2. Creates a set of empty "dynamic" splits, that each consume as many chunks
 *    as it can.
 *    创建一组空的动态分片，让每个消费者尽可能的消费跟多的 chunk 分片
 * This arrangement ensures that a single slow mapper won't slow down the entire
 * job (since the slack will be picked up by other mappers, who consume more
 * chunks.)
 * 这样确保某个慢的 MapTask 影响整 job 的进度
 * By varying the split-ratio, one can vary chunk sizes to achieve different
 * performance characteristics.
 * 通过修改分割比，来慢不同数据块大小的场景
 * 动态分片
 */
public class DynamicInputFormat<K, V> extends InputFormat<K, V> {
  private static final Log LOG = LogFactory.getLog(DynamicInputFormat.class);

  private static final String CONF_LABEL_LISTING_SPLIT_RATIO
          = "mapred.listing.split.ratio";
  private static final String CONF_LABEL_NUM_SPLITS
          = "mapred.num.splits";
  private static final String CONF_LABEL_NUM_ENTRIES_PER_CHUNK
          = "mapred.num.entries.per.chunk";
  private DynamicInputChunkContext<K, V> chunkContext = null;

  /**
   * Implementation of InputFormat::getSplits(). This method splits up the
   * copy-listing file into chunks, and assigns the first batch to different
   * tasks.
   * @param jobContext JobContext for the map job.
   * @return The list of (empty) dynamic input-splits.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    LOG.info("DynamicInputFormat: Getting splits for job:"
             + jobContext.getJobID());
    chunkContext = getChunkContext(jobContext.getConfiguration());
    return createSplits(jobContext,
                        splitCopyListingIntoChunksWithShuffle(jobContext));
  }


  private List<InputSplit> createSplits(JobContext jobContext,
                                        List<DynamicInputChunk> chunks)
          throws IOException {
    //总mapTask 个数
    int numMaps = getNumMapTasks(jobContext.getConfiguration());
    //最终分片个数
    final int nSplits = Math.min(numMaps, chunks.size());
    List<InputSplit> splits = new ArrayList<InputSplit>(nSplits);
    for (int i=0; i< nSplits; ++i) {
      //任务ID
      TaskID taskId = new TaskID(jobContext.getJobID(), TaskType.MAP, i);
      chunks.get(i).assignTo(taskId);
      //初始化默认每个 mapTask 给一个 chunk，读取其前 5 条数据
      splits.add(new FileSplit(chunks.get(i).getPath(), 0,
          // Setting non-zero length for FileSplit size, to avoid a possible
          // future when 0-sized file-splits are considered "empty" and skipped
          // over.
          getMinRecordsPerChunk(jobContext.getConfiguration()),
          null));
    }
    DistCpUtils.publish(jobContext.getConfiguration(),
                        CONF_LABEL_NUM_SPLITS, splits.size());
    return splits;
  }

  private static int N_CHUNKS_OPEN_AT_ONCE_DEFAULT = 16;

  public  DynamicInputChunkContext<K, V> getChunkContext(
      Configuration configuration) throws IOException {
    if(chunkContext == null) {
      chunkContext = new DynamicInputChunkContext<K, V>(configuration);
    }
    return chunkContext;
  }

  /**
   * 根据总文件个数和 mapTask 个数等约束条件，将总文件数切分为多个 chunk
   * @param context
   * */
  private List<DynamicInputChunk> splitCopyListingIntoChunksWithShuffle
                                    (JobContext context) throws IOException {

    final Configuration configuration = context.getConfiguration();
    //待拷贝的文件总数 （645）
    int numRecords = getNumberOfRecords(configuration);
    //总mapTask 个数
    int numMaps = getNumMapTasks(configuration);
    //每个 mapTask 最多能处理的 chunk 个数（400）
    int maxChunksTolerable = getMaxChunksTolerable(configuration);

    // Number of chunks each map will process, on average.
    //理想情况下每个 mapTask 处理 Chunk 个数（20）
    int splitRatio = getListingSplitRatio(configuration, numMaps, numRecords);
    //校验：平均每个 mapTask 处理的 Chunk 个数，不能超过最大允许的 Chunk 个数 （maxChunksTolerable）
    validateNumChunksUsing(splitRatio, numMaps, maxChunksTolerable);

     //每个 chunk 文件中记录需要处理的文件数（7）
    int numEntriesPerChunk = (int)Math.ceil((float)numRecords
                                          /(splitRatio * numMaps));
    DistCpUtils.publish(context.getConfiguration(),
                        CONF_LABEL_NUM_ENTRIES_PER_CHUNK,
                        numEntriesPerChunk);
    //总共需要切分的 chunk 文件数（92）
    final int nChunksTotal = (int)Math.ceil((float)numRecords/numEntriesPerChunk);

     //参与洗牌的 Chunk 个数（作为下限，最少 16 个Chunk 参与洗牌）
    int nChunksOpenAtOnce
            = Math.min(N_CHUNKS_OPEN_AT_ONCE_DEFAULT, nChunksTotal);

    //待分片的文件列表
    Path listingPath = getListingFilePath(configuration);

    SequenceFile.Reader reader
            = new SequenceFile.Reader(configuration,
                                      SequenceFile.Reader.file(listingPath));

    List<DynamicInputChunk> openChunks
                  = new ArrayList<DynamicInputChunk>();
    //最终生产的所有 Chunk 文件
    List<DynamicInputChunk> chunksFinal = new ArrayList<DynamicInputChunk>();

    CopyListingFileStatus fileStatus = new CopyListingFileStatus();
    Text relPath = new Text();
    //文件计数器，为了洗牌使用
    int recordCounter = 0;
    //一轮洗牌创建的 chunk 个数
    int chunkCount = 0;

    try {

      while (reader.next(relPath, fileStatus)) {
        //按照一轮最多处理的文件数，将文件拆解到不同的 chunk 文件中
        // (nChunksOpenAtOnce*numEntriesPerChunk = 16 *7 = 112 参与一轮洗牌的文件个数)
        if (recordCounter % (nChunksOpenAtOnce*numEntriesPerChunk) == 0) {
          // All chunks full. Create new chunk-set.
          closeAll(openChunks); //关闭所有已经分配好文件的chunk
          chunksFinal.addAll(openChunks);//将已经处理完的chunk 添加到 chunksFinal 中

          openChunks = createChunks(chunkCount, nChunksTotal,
              nChunksOpenAtOnce);

          chunkCount += openChunks.size();

          nChunksOpenAtOnce = openChunks.size();
          recordCounter = 0;
        }

        // Shuffle into open chunks. 需要处理的文件洗牌后写入，不同的 chunk 文件中
        openChunks.get(recordCounter%nChunksOpenAtOnce).write(relPath, fileStatus);
        ++recordCounter;
      }

    } finally {
      closeAll(openChunks);
      chunksFinal.addAll(openChunks);
      IOUtils.closeStream(reader);
    }

    LOG.info("Number of dynamic-chunk-files created: " + chunksFinal.size()); 
    return chunksFinal;
  }

  private static void validateNumChunksUsing(int splitRatio, int numMaps,
      int maxChunksTolerable) throws IOException {
    if (splitRatio * numMaps > maxChunksTolerable)
      throw new IOException("Too many chunks created with splitRatio:"
                 + splitRatio + ", numMaps:" + numMaps
                 + ". Reduce numMaps or decrease split-ratio to proceed.");
  }

  private static void closeAll(List<DynamicInputChunk> chunks) {
    for (DynamicInputChunk chunk: chunks)
      chunk.close();
  }

  private List<DynamicInputChunk> createChunks(int chunkCount,
      int nChunksTotal, int nChunksOpenAtOnce)
      throws IOException {
    List<DynamicInputChunk> chunks = new ArrayList<DynamicInputChunk>();
    // 当前批次处理文件的上限 （16）
    // chunkCount 为上一轮生成的 chunk 文件数
    int chunkIdUpperBound
            = Math.min(nChunksTotal, chunkCount + nChunksOpenAtOnce);

    // If there will be fewer than nChunksOpenAtOnce chunks left after
    // the current batch of chunks, fold the remaining chunks into
    // the current batch.
    //如果剩余的文件数小于 nChunksOpenAtOnce 则将剩余的文件数也分配到当前批次中处理
    if (nChunksTotal - chunkIdUpperBound < nChunksOpenAtOnce)
      chunkIdUpperBound = nChunksTotal;
     //创建chunkIdUpperBound （16）个文件
    for (int i=chunkCount; i < chunkIdUpperBound; ++i)
      chunks.add(createChunk(i));
    return chunks;
  }

  private DynamicInputChunk createChunk(int chunkId)
                                              throws IOException {
    return chunkContext.createChunkForWrite(String.format("%05d", chunkId));
  }


  private static Path getListingFilePath(Configuration configuration) {
    String listingFilePathString = configuration.get(
            DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, "");

    assert !listingFilePathString.equals("") : "Listing file not found.";

    Path listingFilePath = new Path(listingFilePathString);
    try {
      assert listingFilePath.getFileSystem(configuration)
              .exists(listingFilePath) : "Listing file: " + listingFilePath +
                                          " not found.";
    } catch (IOException e) {
      assert false :   "Listing file: " + listingFilePath
                    + " couldn't be accessed. " + e.getMessage();
    }
    return listingFilePath;
  }

  private static int getNumberOfRecords(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS);
  }

  private static int getNumMapTasks(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              JobContext.NUM_MAPS);
  }

  private static int getListingSplitRatio(Configuration configuration,
                                            int numMaps, int numPaths) {
    return configuration.getInt(
            CONF_LABEL_LISTING_SPLIT_RATIO,
            getSplitRatio(numMaps, numPaths, configuration));
  }
  
  private static int getMaxChunksTolerable(Configuration conf) {
    int maxChunksTolerable = conf.getInt( // distcp.dynamic.max.chunks.tolerable 默认值 400
        DistCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE,
        DistCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT);
    if (maxChunksTolerable <= 0) {
      LOG.warn(DistCpConstants.CONF_LABEL_MAX_CHUNKS_TOLERABLE +
          " should be positive. Fall back to default value: "
          + DistCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT);
      maxChunksTolerable = DistCpConstants.MAX_CHUNKS_TOLERABLE_DEFAULT;
    }
    return maxChunksTolerable;
  }
  
  private static int getMaxChunksIdeal(Configuration conf) {
    int maxChunksIdeal = conf.getInt( // distcp.dynamic.max.chunks.ideal 默认 100
        DistCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL,
        DistCpConstants.MAX_CHUNKS_IDEAL_DEFAULT);
    if (maxChunksIdeal <= 0) {
      LOG.warn(DistCpConstants.CONF_LABEL_MAX_CHUNKS_IDEAL +
          " should be positive. Fall back to default value: "
          + DistCpConstants.MAX_CHUNKS_IDEAL_DEFAULT);
      maxChunksIdeal = DistCpConstants.MAX_CHUNKS_IDEAL_DEFAULT;
    }
    return maxChunksIdeal;
  }
  
  private static int getMinRecordsPerChunk(Configuration conf) {
    int minRecordsPerChunk = conf.getInt( //distcp.dynamic.min.records_per_chunk 默认: 5
        DistCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK,
        DistCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT);
    if (minRecordsPerChunk <= 0) {
      LOG.warn(DistCpConstants.CONF_LABEL_MIN_RECORDS_PER_CHUNK +
          " should be positive. Fall back to default value: "
          + DistCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT);
      minRecordsPerChunk = DistCpConstants.MIN_RECORDS_PER_CHUNK_DEFAULT;
    }
    return minRecordsPerChunk;
  }

  private static int getSplitRatio(Configuration conf) {
    int splitRatio = conf.getInt(//distcp.dynamic.split.ratio 默认：2
        DistCpConstants.CONF_LABEL_SPLIT_RATIO,
        DistCpConstants.SPLIT_RATIO_DEFAULT);
    if (splitRatio <= 0) {
      LOG.warn(DistCpConstants.CONF_LABEL_SPLIT_RATIO +
          " should be positive. Fall back to default value: "
          + DistCpConstants.SPLIT_RATIO_DEFAULT);
      splitRatio = DistCpConstants.SPLIT_RATIO_DEFAULT;
    }
    return splitRatio;
  }
  
  /**
   * Package private, for testability.
   * @param nMaps The number of maps requested for.
   * @param nRecords The number of records to be copied.
   * @return The number of splits each map should handle, ideally.
   */
  static int getSplitRatio(int nMaps, int nRecords) {
    return getSplitRatio(nMaps, nRecords,new Configuration());
  }
  
  /**
   * Package private, for testability.
   * @param nMaps The number of maps requested for.
   * @param nRecords The number of records to be copied.
   * @param conf The configuration set by users.
   * @return The number of splits each map should handle, ideally.
   * 返回每个 MapTask 处理的 Chunk 个数
   */
  static int getSplitRatio(int nMaps, int nRecords, Configuration conf) {
    //所有 MapTask 最多允许处理的 Chunk 个数，默认: 100
    int maxChunksIdeal = getMaxChunksIdeal(conf);
    //每个 chunk 最少包含的文件个数 默认: 5
    int minRecordsPerChunk = getMinRecordsPerChunk(conf);
    //默认每个 mapTask 处理的 Chunk 个数，默认： 2
    int splitRatio = getSplitRatio(conf);
    //MapTask 个数为 1 ，处理比例为 1
    if (nMaps == 1) {
      LOG.warn("nMaps == 1. Why use DynamicInputFormat?");
      return 1;
    }
    //如果 nMaps 大于  maxChunksIdeal ，则返回默认的 splitRatio
    if (nMaps > maxChunksIdeal)
      return splitRatio;

    //（Ideal）最大限制情况下，平均每个MapTask 处理的 chunk 数量
    int nPickups = (int)Math.ceil((float)maxChunksIdeal/nMaps);
    //（Ideal）最大限制情况下，每个 chunk 中平均包含的文件数
    int nRecordsPerChunk = (int)Math.ceil((float)nRecords/(nMaps*nPickups));
    //如果每个 chunk 中包含的文件数小于 minRecordsPerChunk ，则返回默认的 splitRatio
    return nRecordsPerChunk < minRecordsPerChunk ?
              splitRatio : nPickups;
  }

  static int getNumEntriesPerChunk(Configuration configuration) {
    return DistCpUtils.getInt(configuration,
                              CONF_LABEL_NUM_ENTRIES_PER_CHUNK);
  }


  /**
   * Implementation of Inputformat::createRecordReader().
   * @param inputSplit The split for which the RecordReader is required.
   * @param taskAttemptContext TaskAttemptContext for the current attempt.
   * @return DynamicRecordReader instance.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public RecordReader<K, V> createRecordReader(
          InputSplit inputSplit,
          TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
    chunkContext = getChunkContext(taskAttemptContext.getConfiguration());
    return new DynamicRecordReader<K, V>(chunkContext);
  }
}
