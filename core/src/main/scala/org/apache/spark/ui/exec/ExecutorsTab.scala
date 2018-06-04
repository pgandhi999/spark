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

package org.apache.spark.ui.exec

import scala.collection.mutable
import scala.collection.mutable.{LinkedHashMap, ListBuffer}

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {
  val listener = parent.executorsListener
  val sc = parent.sc
  val threadDumpEnabled =
    sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

  attachPage(new ExecutorsPage(this, threadDumpEnabled))
  if (threadDumpEnabled) {
    attachPage(new ExecutorThreadDumpPage(this))
  }
}

private[ui] case class ExecutorTaskSummary(
    var executorId: String,
    var totalCores: Int = 0,
    var tasksMax: Int = 0,
    var tasksActive: Int = 0,
    var tasksFailed: Int = 0,
    var tasksComplete: Int = 0,
    var duration: Long = 0L,
    var jvmGCTime: Long = 0L,
    var inputBytes: Long = 0L,
    var inputRecords: Long = 0L,
    var outputBytes: Long = 0L,
    var outputRecords: Long = 0L,
    var shuffleRead: Long = 0L,
    var shuffleWrite: Long = 0L,
    var executorLogs: Map[String, String] = Map.empty,
    var isAlive: Boolean = true,
    var isBlacklisted: Boolean = false
)

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the ExecutorsTab
 */
@DeveloperApi
@deprecated("This class will be removed in a future release.", "2.2.0")
class ExecutorsListener(storageStatusListener: StorageStatusListener, conf: SparkConf)
    extends SparkListener {
  val executorToTaskSummary = LinkedHashMap[String, ExecutorTaskSummary]()
  var executorEvents = new ListBuffer[SparkListenerEvent]()
  val executorIdToAddress = mutable.HashMap[String, String]()

  private val maxTimelineExecutors = conf.getInt("spark.ui.timeline.executors.maximum", 1000)
  private val retainedDeadExecutors = conf.getInt("spark.ui.retainedDeadExecutors", 100)

  def activeStorageStatusList: Seq[StorageStatus] = storageStatusListener.storageStatusList

  private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors") {

    init()

    private def init(): Unit = {
      val threadDumpEnabled =
        parent.sc.isDefined && parent.conf.getBoolean("spark.ui.threadDumpsEnabled", true)

      attachPage(new ExecutorsPage(this, threadDumpEnabled))
      if (threadDumpEnabled) {
        attachPage(new ExecutorThreadDumpPage(this, parent.sc))
      }
    }

    private def updateExecutorBlacklist(
      eid: String,
      isBlacklisted: Boolean): Unit = {
      val execTaskSummary = executorToTaskSummary.getOrElseUpdate(eid, ExecutorTaskSummary(eid))
      execTaskSummary.isBlacklisted = isBlacklisted
    }

    def getExecutorHost(eid: String): String = {
      val host = activeStorageStatusList.find { id =>
        id.blockManagerId.executorId == eid
      }
      if (host.nonEmpty) {
        return host.head.blockManagerId.hostPort
      } else {
        return "CANNOT FIND ADDRESS"
      }
    }

    override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted)
    : Unit = synchronized {
      updateExecutorBlacklist(executorBlacklisted.executorId, true)
    }

    override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted)
    : Unit = synchronized {
      updateExecutorBlacklist(executorUnblacklisted.executorId, false)
    }

    override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted)
    : Unit = synchronized {
      // Implicitly blacklist every executor associated with this node, and show this in the UI.
      activeStorageStatusList.foreach { status =>
        if (status.blockManagerId.host == nodeBlacklisted.hostId) {
          updateExecutorBlacklist(status.blockManagerId.executorId, true)
        }
      }
    }
  }

  private[ui] class ExecutorsPage(
    parent: SparkUITab,
    threadDumpEnabled: Boolean)
    extends WebUIPage("") {

    def render(request: HttpServletRequest): Seq[Node] = {
      val content =
        <div>
          {<div id="active-executors" class="row-fluid"></div> ++
          <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
          <script>setThreadDumpEnabled(
            {threadDumpEnabled}
            )</script>}
        </div>

      UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
    }
  }
}
