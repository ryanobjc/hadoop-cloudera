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
package org.apache.hadoop.hdfs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hdfs.DFSClient.BlockReader;

import org.apache.commons.logging.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

/** This is a local block reader. if the DFS client is on
 * the same machine as the datanode, then the client can read
 * files directly from the local file system rathen than going
 * thorugh the datanode. This dramatically improves performance.
 */
public class BlockReaderLocal extends BlockReader {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  private Configuration conf;
  private long startOffset;
  private long length;
  private BlockPathInfo pathinfo;
  private FileInputStream fin;  // reader for the data file
  
  /**
   * The only way this object can be instantiated.
   */
  public static BlockReader newBlockReader(Configuration conf,
    String file, Block blk, DatanodeInfo node, 
    long startOffset, long length) throws IOException {

    // make RPC to local datanode to find local pathnames of blocks
    ClientDatanodeProtocol datanode = null;
    try {
      datanode = DFSClient.createClientDatanodeProtocolProxy(node, conf);
      BlockPathInfo pathinfo = datanode.getBlockPathInfo(blk);
      LOG.warn("pathinfo: " + pathinfo);
      return new BlockReaderLocal(conf, file, blk, startOffset, length,
                                  pathinfo);
    } finally {
      if (datanode != null) {
        RPC.stopProxy(datanode);
      }
    }
  }

  private BlockReaderLocal(Configuration conf, String hdfsfile, Block blk,
                          long startOffset, long length,
                          BlockPathInfo pathinfo) 
                          throws IOException {
    super(hdfsfile, 1);
    this.pathinfo = pathinfo;
    this.startOffset = startOffset;
    this.length = length;    

    // get a local file system
    File blkfile = new File(pathinfo.getBlockPath());
    fin = new FileInputStream(blkfile);
    fin.getChannel().position(startOffset);
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem constructor for file " +
                blkfile + " of size " + blkfile.length() +
                " startOffset " + startOffset +
                " length " + length);
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len)
                               throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem read off " + off + " len " + len);
    }
    return fin.read(buf, off, len);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem skip " + n);
    }
    return fin.skip(n);
  }

  @Override
  public synchronized void seek(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem seek " + n);
    }
    throw new IOException("Seek() is not supported in BlockReaderLocal");
  }

  @Override
  protected synchronized int readChunk(long pos, byte[] buf, int offset,
                                       int len, byte[] checksumBuf)
                                       throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem readChunk pos " + pos +
                " offset " + offset + " len " + len);
    }
    throw new IOException("readChunk() is not supported in BlockReaderLocal");
  }

  @Override
  public synchronized void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("BlockChecksumFileSystem close");
    }
    fin.close();
  }
}

