/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// scalastyle:off
package org.apache.spark.broadcast

import java.io._
import java.lang.ref.SoftReference
import java.nio.ByteBuffer
import java.util.zip.Adler32
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark._
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{KeyLock, Utils}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import java.nio.channels.Channels

/**
 * A BitTorrent-like implementation of [[org.apache.spark.broadcast.Broadcast]].
 *
 * The mechanism is as follows:
 *
 * The driver divides the serialized object into small chunks and
 * stores those chunks in the BlockManager of the driver.
 *
 * On each executor, the executor first attempts to fetch the object from its BlockManager. If
 * it does not exist, it then uses remote fetches to fetch the small chunks from the driver and/or
 * other executors if available. Once it gets the chunks, it puts the chunks in its own
 * BlockManager, ready for other executors to fetch from.
 *
 * This prevents the driver from being the bottleneck in sending out multiple copies of the
 * broadcast data (one per executor).
 *
 * When initialized, TorrentBroadcast objects read SparkEnv.get.conf.
 *
 * @param obj object to broadcast
 * @param id A unique identifier for the broadcast variable.
 */
private[spark] class TorrentBroadcast[T: ClassTag](obj: T, id: Long, isSeanet: Boolean = false)
  extends Broadcast[T](id) with Logging with Serializable {
    // seanet here 14/3/2023 readblocks fails the key here is: The driver divides the serialized object into small chunks and stores those chunks in the BlockManager of the driver
  /**
   * Value of the broadcast object on executors. This is reconstructed by [[readBroadcastBlock]],
   * which builds this value by reading blocks from the driver and/or other executors.
   *
   * On the driver, if the value is required, it is read lazily from the block manager. We hold
   * a soft reference so that it can be garbage collected if required, as we can always reconstruct
   * in the future.
   */
  @transient private var _value: SoftReference[T] = _

  /** The compression codec to use, or None if compression is disabled */
  @transient private var compressionCodec: Option[CompressionCodec] = _
  /** Size of each block. Default value is 4MB.  This value is only read by the broadcaster. */
  @transient private var blockSize: Int = _


  /** Whether to generate checksum for blocks or not. */
  private var checksumEnabled: Boolean = false

  private def setConf(conf: SparkConf): Unit = {
    compressionCodec = if (conf.get(config.BROADCAST_COMPRESS)) {
      Some(CompressionCodec.createCodec(conf))
    } else {
      None
    }
    // Note: use getSizeAsKb (not bytes) to maintain compatibility if no units are provided
    blockSize = conf.get(config.BROADCAST_BLOCKSIZE).toInt * 1024
    checksumEnabled = conf.get(config.BROADCAST_CHECKSUM)
  }
  setConf(SparkEnv.get.conf)

  private val broadcastId = BroadcastBlockId(id)


  /** Total number of blocks this broadcast variable contains. */
    private val numBlocks: Int = { if (!isSeanet) {writeBlocks(obj)} else 0}
  /** The checksum for all the blocks. */
  private var checksums: Array[Int] = _

  override protected def getValue() = synchronized {
      val memoized: T = if (_value == null) null.asInstanceOf[T] else _value.get
      if (memoized != null) {
        memoized
      } else {
        System.out.println("seanet this thing finds memoized null")
        val newlyRead = readBroadcastBlock()
        _value = new SoftReference[T](newlyRead)
        System.out.println("seanet getvalue returns " +  newlyRead.getClass + " for id " + id + " broadcastId " + broadcastId)
        newlyRead
      }

  }

  private def calcChecksum(block: ByteBuffer): Int = {
    val adler = new Adler32()
    if (block.hasArray) {
      adler.update(block.array, block.arrayOffset + block.position(), block.limit()
        - block.position())
    } else {
      val bytes = new Array[Byte](block.remaining())
      block.duplicate.get(bytes)
      adler.update(bytes)
    }
    adler.getValue.toInt
  }

  /**
   * Divide the object into multiple blocks and put those blocks in the block manager.
   *
   * @param value the object to divide
   * @return number of blocks this broadcast variable is divided into
   */
  private def writeBlocks(value: T): Int = {
    import StorageLevel._
    System.out.println("seanet write blocks")
    // Store a copy of the broadcast variable in the driver so that tasks run on the driver
    // do not create a duplicate copy of the broadcast variable's value.
    val blockManager = SparkEnv.get.blockManager
    if (blockManager.map.contains(broadcastId)) {
      System.out.println("seanet write blocks found broadcast so returning it")
      var lala : Int =  blockManager.map.get(broadcastId).get
      lala
    } else {
      if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
        throw new SparkException(s"Failed to store $broadcastId in BlockManager")
      }
      try {
        System.out.println("seanet writeblocks SparkEnv.get.serializer: " + SparkEnv.get.serializer)

        val blocks =
          TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
        if (checksumEnabled) {
          checksums = new Array[Int](blocks.length)
        }

        System.out.println("seanet writeblocks blockifyObject after")

        blocks.zipWithIndex.foreach { case (block, i) =>
          if (checksumEnabled) {
            checksums(i) = calcChecksum(block)
          }
          val pieceId = BroadcastBlockId(id, "piece" + i)
          val bytes = new ChunkedByteBuffer(block.duplicate())
          // seanet here 17/3/2023 should i also save the blocks ?
          if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
            throw new SparkException(s"Failed to store $pieceId of $broadcastId " +
              s"in local BlockManager")
          }
        }


          System.out.println("seanet writeblocks putBytes after")
          blockManager.map.put(broadcastId, blocks.length)

        blocks.length

      } catch {
        case t: Throwable =>
          logError(s"Store broadcast $broadcastId fail, remove all pieces of the broadcast")
          blockManager.removeBroadcast(id, tellMaster = true)
          throw t
      }
    }
  }


  /** Fetch torrent blocks from the driver and/or other executors. */
  private def readBlocks(): Array[BlockData] = {
    System.out.println("seanet readBlocks")

    if (new File("../../../seanet").exists()) {
      writeBlocks(obj)
    }

    // Fetch chunks of data. Note that all these chunks are stored in the BlockManager and reported
    // to the driver, so other executors can pull these chunks from this executor as well.
    val blocks = new Array[BlockData](numBlocks)
    val bm = SparkEnv.get.blockManager

    for (pid <- Random.shuffle(Seq.range(0, numBlocks))) {
      val pieceId = BroadcastBlockId(id, "piece" + pid)
      logDebug(s"Reading piece $pieceId of $broadcastId")
      // First try getLocalBytes because there is a chance that previous attempts to fetch the
      // broadcast blocks have already fetched some of the blocks. In that case, some blocks
      // would be available locally (on this executor).
      System.out.println("seanet readBlocks calling getLocalBytes")
      bm.getLocalBytes(pieceId) match {
        case Some(block) =>
          blocks(pid) = block
          releaseBlockManagerLock(pieceId)
        case None =>
          bm.getRemoteBytes(pieceId) match {
            case Some(b) =>
              if (checksumEnabled) {
                val sum = calcChecksum(b.chunks(0))
                if (sum != checksums(pid)) {
                  throw new SparkException(s"corrupt remote block $pieceId of $broadcastId:" +
                    s" $sum != ${checksums(pid)}")
                }
              }
              // We found the block from remote executors/driver's BlockManager, so put the block
              // in this executor's BlockManager.
              if (!bm.putBytes(pieceId, b, StorageLevel.MEMORY_AND_DISK_SER, tellMaster = true)) {
                throw new SparkException(
                  s"Failed to store $pieceId of $broadcastId in local BlockManager")
              }
              blocks(pid) = new ByteBufferBlockData(b, true)
            case None =>
              throw new SparkException(s"Failed to get $pieceId of $broadcastId")
          }
      }
    }
    blocks
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
   * Remove all persisted state associated with this Torrent broadcast on the executors
   * and driver.
   */
  override protected def doDestroy(blocking: Boolean): Unit = {
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }

  private def readBroadcastBlock(): T = {

    System.out.println("readBroadcastBlock start")
    TorrentBroadcast.torrentBroadcastLock.withLock(broadcastId) {
      // As we only lock based on `broadcastId`, whenever using `broadcastCache`, we should only
      // touch `broadcastId`.
      val broadcastCache = SparkEnv.get.broadcastManager.cachedValues

      // seanet here maybe when i know more i will fix this
      //Option(SparkEnv.get.broadcastManager.get(broadcastId)).map(_.asInstanceOf[T]).getOrElse {
        System.out.println("readBroadcastBlock orelse")
        setConf(SparkEnv.get.conf)
        val blockManager = SparkEnv.get.blockManager

        blockManager.getLocalValues(broadcastId) match {
          case Some(blockResult) =>
            System.out.println("readBroadcastBlock Some(blockResult)")
            if (blockResult.data.hasNext) {
              System.out.println("seanet readBroadcastBlock blockResult.data.hasNext")
              val x = blockResult.data.next().asInstanceOf[T]
              releaseBlockManagerLock(broadcastId)

              if (x != null) {
                System.out.println("seanet readBroadcastBlock x!=null")
                broadcastCache.put(broadcastId, x)
              }
              System.out.println("seanet readBroadcastBlock returning " + x.getClass)
              x
            } else {
              throw new SparkException(s"Failed to get locally stored broadcast data: $broadcastId")
            }
          case None =>
            System.out.println("readBroadcastBlock none")
            val estimatedTotalSize = Utils.bytesToString(numBlocks * blockSize)
            logInfo(s"Started reading broadcast variable $id with $numBlocks pieces " +
              s"(estimated total size $estimatedTotalSize)")
            val startTimeNs = System.nanoTime()
            val blocks = readBlocks()
            logInfo(s"Reading broadcast variable $id took ${Utils.getUsedTimeNs(startTimeNs)}")

            try {
              var obj: Option[T] = null
              if (new File("../../../seanet").exists()) {
                obj = Option(TorrentBroadcast.unBlockifyObject[T](
                  blocks.map(_.toInputStream()), SparkEnv.get.serializer, None ))
              } else {
              obj = Option(TorrentBroadcast.unBlockifyObject[T](
                blocks.map(_.toInputStream()), SparkEnv.get.serializer, compressionCodec ))
              }
              // Store the merged copy in BlockManager so other tasks on this executor don't
              // need to re-fetch it.
              if (!new File("../../../seanet").exists()) {
                val storageLevel = StorageLevel.MEMORY_AND_DISK
                if (!blockManager.putSingle(broadcastId, obj.get, storageLevel, tellMaster = false)) {
                  throw new SparkException(s"Failed to store $broadcastId in BlockManager")
                }

                if (obj.get != null) {
                  broadcastCache.put(broadcastId, obj)
                }
              }
              obj.get
            } finally {
              blocks.foreach(_.dispose())
            }
        }
      //}
    }
  }

  private def readObject(in: ObjectInputStream): ByteBuffer = Utils.tryOrIOException {
    val length = in.readInt()
    var buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  /**
   * If running in a task, register the given block's locks for release upon task completion.
   * Otherwise, if not running in a task then immediately release the lock.
   */
  private def releaseBlockManagerLock(blockId: BlockId): Unit = {
    val blockManager = SparkEnv.get.blockManager
    Option(TaskContext.get()) match {
      case Some(taskContext) =>
        taskContext.addTaskCompletionListener[Unit](_ => blockManager.releaseLock(blockId))
      case None =>
        // This should only happen on the driver, where broadcast variables may be accessed
        // outside of running tasks (e.g. when computing rdd.partitions()). In order to allow
        // broadcast variables to be garbage collected we need to free the reference here
        // which is slightly unsafe but is technically okay because broadcast variables aren't
        // stored off-heap.
        blockManager.releaseLock(blockId)
    }
  }

}


private object TorrentBroadcast extends Logging {

  /**
   * A [[KeyLock]] whose key is [[BroadcastBlockId]] to ensure there is only one thread fetching
   * the same [[TorrentBroadcast]] block.
   */
  private val torrentBroadcastLock = new KeyLock[BroadcastBlockId]

  def blockifyObject[T: ClassTag](
      obj: T,
      blockSize: Int,
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): Array[ByteBuffer] = {

    System.out.println("seanet blockifyObject object start")
    val cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer.allocate)

    var out: OutputStream = null;
    if (!new File("../../../seanet").exists()) {
      System.out.println("seanet blockifyObject object mid 1")
      out = compressionCodec.map(c => c.compressedOutputStream(cbbos)).getOrElse(cbbos)
    } else {
      System.out.println("seanet blockifyObject object mid 2")
      out = cbbos
    }

    val ser = serializer.newInstance()
    System.out.println("seanet blockifyObject object mid 3")
    val serOut = ser.serializeStream(out)
    System.out.println("seanet blockifyObject object mid 4")
    Utils.tryWithSafeFinally {
      serOut.writeObject[T](obj)
      System.out.println("seanet blockifyObject object mid 5")
    } {
      serOut.close()
      System.out.println("seanet blockifyObject object mid 6")
    }
    System.out.println("seanet blockifyObject object mid 7")
    cbbos.toChunkedByteBuffer.getChunks()
  }

  def unBlockifyObject[T: ClassTag](
      blocks: Array[InputStream],
      serializer: Serializer,
      compressionCodec: Option[CompressionCodec]): T = {
    require(blocks.nonEmpty, "Cannot unblockify an empty array of blocks")
    val is = new SequenceInputStream(blocks.iterator.asJavaEnumeration)
    val in: InputStream = compressionCodec.map(c => c.compressedInputStream(is)).getOrElse(is)
    val ser = serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
    obj
  }

  /**
   * Remove all persisted blocks associated with this torrent broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = {
    logDebug(s"Unpersisting TorrentBroadcast $id")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
  }
}
