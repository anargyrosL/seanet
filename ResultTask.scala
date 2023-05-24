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
package org.apache.spark.scheduler
import java.lang.reflect.Field
import java.io._
import java.lang.invoke.SerializedLambda
import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.{RDD, MapPartitionsRDD}
import java.lang.invoke.SerializedLambda
import java.nio.channels.Channels
import java.nio.file.Paths
import org.apache.spark.rdd.MapPartitionsRDD


/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    locs: Seq[TaskLocation],
    val outputId: Int,
    localProperties: Properties,
    serializedTaskMetrics: Array[Byte],
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None,
    isBarrier: Boolean = false)
  extends Task[U](stageId, stageAttemptId, partition.index, localProperties, serializedTaskMetrics,
    jobId, appId, appAttemptId, isBarrier)
  with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.distinct
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTimeNs = System.nanoTime()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val myDir2 = new File("../../../seanet")
    System.out.println(" mesa sto result eimai sto:  " + Paths.get("").toAbsolutePath().toString() + " kai to seanet folder exists: " + myDir2.exists())
    if (myDir2.exists()) {
      val myDir = new File("../../../broadcasts")
      val binFolder = new File("../../../binFiles")

      var broadcastFiles: List[File] = myDir.listFiles().filter(name => name.getName.startsWith("stagesntRDDActualBoth_" + stageId)).sorted.toList

      System.out.println("seanet list of broacasted files " + broadcastFiles + "  stage id is:" + stageId + " size of list is  " + broadcastFiles.size)
      var obs = new ObjectInputStream(new FileInputStream(broadcastFiles.apply(0)))
      var both = obs.readObject().asInstanceOf[Broadcast[Array[Byte]]];
      obs.close()

      val fileDesc = new File("../../../broadcasts/filedesc_" + stageId +  ".bdc")
      val os1ab = new ObjectInputStream(new FileInputStream(fileDesc))
      var rddClass = os1ab.readLine()
      System.out.println("seanet salvation " + rddClass)
      var funcClass = os1ab.readLine()
      System.out.println("seanet salvation " + funcClass)
      os1ab.close()

      broadcastFiles = myDir.listFiles().filter(name => name.getName.startsWith("rddonly_" + stageId)).sorted.toList
      var obs2 = new ObjectInputStream(new FileInputStream(broadcastFiles.apply(0)))
      val rdd2 = obs2.readObject().asInstanceOf[MapPartitionsRDD[_,_]]
      obs2.close()

      // seanet here 11/5
      broadcastFiles = myDir.listFiles().filter(name => name.getName.startsWith("rddfucntiononly_" + stageId)).sorted.toList
      var obs2a = new ObjectInputStream(new FileInputStream(broadcastFiles.apply(0)))
      val obj = obs2a.readObject()
      val fValue = obj.asInstanceOf[(TaskContext, Int, Iterator[Any]) => Iterator[Any]]
      val field: Field = classOf[MapPartitionsRDD[_, _]].getDeclaredField("f")
      field.setAccessible(true)
      field.set(rdd2, fValue) // Replace `newValue` with the deserialized function value
      obs2a.close()


      broadcastFiles = myDir.listFiles().filter(name => name.getName.startsWith("funconly_" + stageId)).sorted.toList
      var obs3 = new ObjectInputStream(new FileInputStream(broadcastFiles.apply(0)))
      val func2 = obs3.readObject().asInstanceOf[(TaskContext, Iterator[T]) => U]
      obs3.close()

      SparkEnv.get.broadcastManager.newBroadcast[Broadcast[Array[Byte]]](both, true)


/*      val binFiles: List[File] = binFolder.listFiles().filter(name => name.getName.startsWith("taskDescBinary")).sorted.toList
      System.out.println("seanet list of taskbinaries files " + binFiles + " size of list is  " + binFiles.size)
      binFiles.foreach(file => {
        val obs = new ObjectInputStream(new FileInputStream(file))
        val taskDesc = TaskDescription.decode(readObject(obs))
        SparkEnv.get.broadcastManager.newBroadcast[TaskDescription](taskDesc, true)

        obs.close()
      })*/

    /*  val factory = new TorrentBroadcastFactory
      val da = factory.newBroadcast(both, true, stageId, true)*/

      /*val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
        ByteBuffer.wrap(both.value), Thread.currentThread.getContextClassLoader)*/


/*      val broadcastFiles2: List[File] = myDir.listFiles().filter(name => name.getName.startsWith("stagesntfunc_" + stageId)).sorted.toList
      val obs2 = new ObjectInputStream(new FileInputStream(broadcastFiles2.apply(0)))
      val func = obs2.readObject().asInstanceOf[(TaskContext, Iterator[_]) => U]
      obs2.close()*/

      /*val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
        ByteBuffer.wrap(broadcastObject.asInstanceOf[Broadcast[Array[Byte]]].value), Thread.currentThread.getContextClassLoader)*/
      _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
      _executorDeserializeCpuTime =
        if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
        } else 0L

      // System.out.println("seanet func(context, rdd.iterator(partition, context)) " + rdd.getClass +" <- rdd class | rdd.iterator ->" +rdd.iterator(partition, context) + " partition: " + partition.getClass)
      func2(context, rdd2.asInstanceOf[MapPartitionsRDD[T,U]].iterator(partition, context))

    } else {
      System.out.println("seanet taskBinary.value class " + taskBinary.value.getClass + " stage is " + stageId + " stageattempt is " + stageAttemptId + " context " + context.partitionId)
      val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
        ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
      System.out.println("seanet taskBinary.value class ENDED")

      val myDir = new File("../../../broadcasts")

      if (rdd.isInstanceOf[MapPartitionsRDD[_,_]]) {
        val broadcastFiles = myDir.listFiles().filter(name => name.getName.startsWith("rddfucntiononly_" + stageId)).sorted.toList
        var obs2a = new ObjectInputStream(new FileInputStream(broadcastFiles.apply(0)))
        val obj = obs2a.readObject()
        val fValue = obj.asInstanceOf[(TaskContext, Int, Iterator[Any]) => Iterator[Any]]
        val field: Field = classOf[MapPartitionsRDD[_, _]].getDeclaredField("f")
        field.setAccessible(true)
        field.set(rdd, fValue) // Replace `newValue` with the deserialized function value
        System.out.println("seanet resulttask wrote fvalue " + fValue)
        System.out.println("seanet resulttask wrote this to rdd as f " + rdd.asInstanceOf[MapPartitionsRDD[_,_]].f)
        obs2a.close()
      }

      _executorDeserializeTimeNs = System.nanoTime() - deserializeStartTimeNs
      _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
          threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
        } else 0L

      func(context, rdd.iterator(partition, context))
    }
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ResultTask(" + stageId + ", " + partitionId + ")"


  private def readObject(in: ObjectInputStream): ByteBuffer =  {
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

  def fromSerializedLambda[T, U](sl: SerializedLambda): (TaskContext, Int, Iterator[T]) => Iterator[U] = {
    val serializedLambda = serialize(sl)
    val deserializedLambda = deserialize[(TaskContext, Int, Iterator[T]) => Iterator[U]](serializedLambda)

    (t1: TaskContext, t2: Int, t3: Iterator[T]) => deserializedLambda(t1, t2, t3)
  }

  def serialize(obj: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(obj)
    oos.close()
    bos.toByteArray
  }

  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    val obj = ois.readObject().asInstanceOf[T]
    ois.close()
    bis.close()
    obj
  }
}
