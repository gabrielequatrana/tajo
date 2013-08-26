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

package org.apache.tajo.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryConf;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.TajoContainerProxy;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.master.rm.TajoWorkerContainer;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.master.rm.WorkerResource;
import org.apache.tajo.rpc.CallFuture2;
import org.apache.tajo.util.ApplicationIdUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  static AtomicInteger containerIdSeq = new AtomicInteger(0);
  private TajoConf tajoConf;
  private QueryMasterTask.QueryContext queryContext;
  private final ExecutorService executorService;

  private AtomicBoolean stopped = new AtomicBoolean(false);

  public TajoResourceAllocator(QueryMasterTask.QueryContext queryContext) {
    this.queryContext = queryContext;
    executorService = Executors.newFixedThreadPool(
        queryContext.getConf().getIntVar(TajoConf.ConfVars.AM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  @Override
  public ContainerId makeContainerId(YarnProtos.ContainerIdProto containerIdProto) {
    TajoWorkerContainerId containerId = new TajoWorkerContainerId();
    ApplicationAttemptId appAttemptId = new ApplicationAttemptIdPBImpl(containerIdProto.getAppAttemptId());
    containerId.setApplicationAttemptId(appAttemptId);
    containerId.setId(containerIdProto.getId());
    return containerId;
  }

  @Override
  public void allocateTaskWorker() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf)conf;

    queryContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());
//
    queryContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());

    super.init(conf);
  }

  @Override
  public void stop() {
    if(stopped.get()) {
      return;
    }
    stopped.set(true);
    executorService.shutdownNow();

    while(!executorService.isTerminated()) {
      LOG.info("====>executorService.isTerminated:" + executorService.isTerminated() + "," +
          executorService.isShutdown());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    Map<ContainerId, ContainerProxy> containers = queryContext.getResourceAllocator().getContainers();
    for(ContainerProxy eachProxy: containers.values()) {
      try {
        eachProxy.stopContainer();
      } catch (Exception e) {
      }
    }
    super.stop();
  }

  @Override
  public void start() {
    super.start();
  }

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  private static void writeConf(Configuration conf, Path queryConfFile)
      throws IOException {
    // Write job file to Tajo's fs
    FileSystem fs = queryConfFile.getFileSystem(conf);
    FSDataOutputStream out =
        FileSystem.create(fs, queryConfFile,
            new FsPermission(QUERYCONF_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        launchTaskRunners(event.getExecutionBlockId(), event.getContainers());
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        stopContainers(event.getContainers());
      }
    }
  }

  private void launchTaskRunners(ExecutionBlockId executionBlockId, Collection<Container> containers) {
    FileSystem fs = null;

    QueryConf queryConf = queryContext.getConf();
    LOG.info("defaultFS: " + queryConf.get("fs.default.name"));
    LOG.info("defaultFS: " + queryConf.get("fs.defaultFS"));
    try {
      fs = FileSystem.get(queryConf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    try {
      // TODO move to tajo temp
      Path warehousePath = new Path(queryConf.getVar(TajoConf.ConfVars.ROOT_DIR), TajoConstants.WAREHOUSE_DIR);
      Path queryConfPath = new Path(warehousePath, executionBlockId.getQueryId().toString());
      queryConfPath = new Path(queryConfPath, QueryConf.FILENAME);

      if(!fs.exists(queryConfPath)){
        LOG.info("Writing a QueryConf to HDFS and add to local environment, outputPath=" + queryConf.getOutputPath());
        writeConf(queryConf, queryConfPath);
      } else {
        LOG.warn("QueryConf already exist. path: "  + queryConfPath.toString());
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
    //Query in standby mode doesn't need launch Worker.
    //But, Assign ExecutionBlock to assigned tajo worker
    for(Container eachContainer: containers) {
      TajoContainerProxy containerProxy = new TajoContainerProxy(queryContext, tajoConf,
          eachContainer, executionBlockId);
      executorService.submit(new LaunchRunner(eachContainer.getId(), containerProxy));
    }
  }

  protected class LaunchRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public LaunchRunner(ContainerId id, ContainerProxy proxy) {
      this.proxy = proxy;
      this.id = id;
    }
    @Override
    public void run() {
      proxy.launch(null);
      LOG.info("ContainerProxy started:" + id);
    }
  }

  private void stopContainers(Collection<Container> containers) {
    for (Container container : containers) {
      final ContainerProxy proxy = queryContext.getResourceAllocator().getContainer(container.getId());
      executorService.submit(new StopContainerRunner(container.getId(), proxy));
    }
  }

  private class StopContainerRunner implements Runnable {
    private final ContainerProxy proxy;
    private final ContainerId id;
    public StopContainerRunner(ContainerId id, ContainerProxy proxy) {
      this.id = id;
      this.proxy = proxy;
    }

    @Override
    public void run() {
      LOG.info("ContainerProxy stopped:" + id + "," + proxy.getId());
      proxy.stopContainer();
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {
    @Override
    public void handle(ContainerAllocationEvent event) {
      executorService.submit(new TajoWorkerAllocationThread(event));
    }
  }

  class TajoWorkerAllocationThread extends Thread {
    ContainerAllocationEvent event;
    TajoWorkerAllocationThread(ContainerAllocationEvent event) {
      this.event = event;
    }

    @Override
    public void run() {
      LOG.info("======> Start TajoWorkerAllocationThread");
      CallFuture2<TajoMasterProtocol.WorkerResourceAllocationResponse> callBack =
          new CallFuture2<TajoMasterProtocol.WorkerResourceAllocationResponse>();

      int requiredMemoryMBSlot = 512;  //TODO
      int requiredDiskSlots = 1;  //TODO
      TajoMasterProtocol.WorkerResourceAllocationRequest request =
          TajoMasterProtocol.WorkerResourceAllocationRequest.newBuilder()
              .setMemoryMBSlots(requiredMemoryMBSlot)
              .setDiskSlots(requiredDiskSlots)
              .setNumWorks(event.getRequiredNum())
              .setExecutionBlockId(event.getExecutionBlockId().getProto())
              .build();

      queryContext.getQueryMasterContext().getWorkerContext().
          getTajoMasterRpcClient().allocateWorkerResources(null, request, callBack);

      int numAllocatedWorkers = 0;
      while(!stopped.get()) {
        TajoMasterProtocol.WorkerResourceAllocationResponse response = null;
        try {
          response = callBack.get(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          if(stopped.get()) {
            break;
          }
        } catch (TimeoutException e) {
          LOG.info("No available worker resource for " + event.getExecutionBlockId());
          continue;
        }

        List<String> workerHosts = response.getAllocatedWorksList();
        ExecutionBlockId executionBlockId = event.getExecutionBlockId();

        List<Container> containers = new ArrayList<Container>();
        for(String eachWorker: workerHosts) {
          TajoWorkerContainer container = new TajoWorkerContainer();
          NodeIdPBImpl nodeId = new NodeIdPBImpl();
          String[] tokens = eachWorker.split(":");

          nodeId.setHost(tokens[0]);
          nodeId.setPort(Integer.parseInt(tokens[1]));

          TajoWorkerContainerId containerId = new TajoWorkerContainerId();

          containerId.setApplicationAttemptId(
              ApplicationIdUtils.createApplicationAttemptId(executionBlockId.getQueryId()));
          containerId.setId(containerIdSeq.incrementAndGet());

          container.setId(containerId);
          container.setNodeId(nodeId);

          WorkerResource workerResource = new WorkerResource();
          workerResource.setAllocatedHost(nodeId.getHost());
          workerResource.setPorts(new int[]{nodeId.getPort()});
          workerResource.setMemoryMBSlots(requiredMemoryMBSlot);
          workerResource.setDiskSlots(requiredDiskSlots);

          container.setWorkerResource(workerResource);

          containers.add(container);
        }

        SubQueryState state = queryContext.getSubQuery(executionBlockId).getState();
        if (!SubQuery.isRunningState(state)) {
          List<WorkerResource> workerResources = new ArrayList<WorkerResource>();
          for(Container eachContainer: containers) {
            workerResources.add(((TajoWorkerContainer)eachContainer).getWorkerResource());
          }
          try {
            TajoContainerProxy.releaseWorkerResource(queryContext, executionBlockId, workerResources);
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
          return;
        }

        if (workerHosts.size() > 0) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);
          }
          queryContext.getEventHandler().handle(new SubQueryContainerAllocationEvent(executionBlockId, containers));
        }
        numAllocatedWorkers += workerHosts.size();
        if(numAllocatedWorkers >= event.getRequiredNum()) {
          break;
        }
      }
      LOG.info("======> Stop TajoWorkerAllocationThread");
    }
  }
}
