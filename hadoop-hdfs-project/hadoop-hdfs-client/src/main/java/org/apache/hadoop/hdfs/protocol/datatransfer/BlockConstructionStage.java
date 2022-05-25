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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Block Construction Stage */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum BlockConstructionStage {
  /** The enumerates are always listed as regular stage followed by the
   * recovery stage.
   * Changing this order will make getRecoveryStage not working.
   */
  // pipeline set up for block append
  PIPELINE_SETUP_APPEND,  //追加写已有文件时，数据流管道的初始状态
  // pipeline set up for failed PIPELINE_SETUP_APPEND recovery
  PIPELINE_SETUP_APPEND_RECOVERY,
  // data streaming
  DATA_STREAMING, // 数据流管道以已经建立好了，可以传输数据
  // pipeline setup for failed data streaming recovery
  PIPELINE_SETUP_STREAMING_RECOVERY,
  // close the block and pipeline
  PIPELINE_CLOSE, //数据块已经写满了，数据管道关闭
  // Recover a failed PIPELINE_CLOSE
  PIPELINE_CLOSE_RECOVERY,
  // pipeline set up for block creation
  PIPELINE_SETUP_CREATE, //  写新文件时，数据管道流的初始状态
  // transfer RBW for adding datanodes
  TRANSFER_RBW,
  // transfer Finalized for adding datanodes
  TRANSFER_FINALIZED;

  final static private byte RECOVERY_BIT = (byte)1;

  /**
   * get the recovery stage of this stage
   */
  public BlockConstructionStage getRecoveryStage() {
    if (this == PIPELINE_SETUP_CREATE) {
      throw new IllegalArgumentException( "Unexpected blockStage " + this);
    } else {
      return values()[ordinal()|RECOVERY_BIT];
    }
  }
}
