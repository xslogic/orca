package org.kuttz.orca;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.kuttz.orca.controller.OrcaController;
import org.kuttz.orca.controller.OrcaController.ControllerRequest;
import org.kuttz.orca.controller.OrcaController.ControllerRequest.ReqType;
import org.kuttz.orca.controller.OrcaController.Node;
import org.kuttz.orca.controller.OrcaControllerArgs;
import org.kuttz.orca.controller.OrcaControllerClient;
import org.kuttz.orca.controller.OrcaLaunchContext;
import org.kuttz.orca.hmon.HeartbeatMasterClient;
import org.kuttz.orca.hmon.HeartbeatNode;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaAppMaster implements OrcaControllerClient, HeartbeatMasterClient {

	private static Logger logger = LoggerFactory.getLogger(OrcaAppMaster.class);
	
	private final OrcaControllerArgs orcaArgs;
	
	private OrcaController oc;
	// Configuration
	private Configuration conf;

	// Handle to communicate with the Resource Manager
	private AMRMClient<ContainerRequest> resourceManager;

	// Application Attempt Id ( combination of attemptId and fail count )
	private ApplicationAttemptId appAttemptID;

	// For status update for clients - yet to be implemented
	// Hostname of the container
	private final String appMasterHostname = "";
	// Port on which the app master listens for status update requests from clients
	private final int appMasterRpcPort = 0;
	
	// Containers to be released
	private final CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();	
	
	private int containerMemory;
	
	private ExecutorService tp = Executors.newCachedThreadPool();
	
	private RequestContainerRunnable requester = new RequestContainerRunnable();
	
	private TrackingService trackingService = new TrackingService();
	
	private NMClient nmClient;
	
	private Random rnd;
	
	@Override
	public HeartbeatMasterClient getHeartbeatClient() {
		return this;
	}

	@Override
	public ExecutorService getExecutorService() {
		return this.tp;
	}
	
	public static void main(String[] args) {
		logger.info("Starting Orca Application Master..");
		
		OrcaControllerArgs orcaArgs = new OrcaControllerArgs();
		logger.info("OrcaAM args = " + Arrays.toString(args));
		Tools.parseArgs(orcaArgs, args);
		
		OrcaAppMaster appMaster = new OrcaAppMaster(orcaArgs);
		try {
			appMaster.init();
			appMaster.run();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error running Orca ApplicationMaster", e);
			System.exit(1);
		}
		logger.info("Orca ApplicationMaster completed successfully..");
		System.exit(0);				
	}	
	
	public OrcaAppMaster(OrcaControllerArgs orcaArgs) {
		this.orcaArgs = orcaArgs;
	}
	
	public void init() throws IOException {
		containerMemory = 256;
		
		Map<String, String> envs = System.getenv();
		
		appAttemptID = Records.newRecord(ApplicationAttemptId.class);
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			throw new IllegalArgumentException("Application Attempt Id not set in the environment");
		} else {
		    ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
		    appAttemptID = containerId.getApplicationAttemptId();
		}		
		
		logger.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
		        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
		        + appAttemptID.getAttemptId());

		conf = new YarnConfiguration();	
		
		this.tp.submit(requester);
		this.tp.submit(trackingService);
		this.oc = new OrcaController(this.orcaArgs, this);
	}
	
	public void run() {
		logger.info("Starting ApplicationMaster");
		
		oc.init();
		this.tp.submit(oc);		
		
		// Connect to ResourceManager
		resourceManager = connectToRM();
		
		nmClient = NMClient.createNMClient();
		nmClient.setNMTokenCache(NMTokenCache.getSingleton());
		nmClient.init(conf);
		nmClient.start();

		// Setup local RPC Server to accept status requests directly from clients
		// TODO need to setup a protocol for client to be able to communicate to the RPC server
		// TODO use the rpc port info to register with the RM for the client to
		// send requests to this app master

		RegisterApplicationMasterResponse response = null;
		try {
		  response = registerToRM();
		} catch (Exception e) {
	        logger.error("Got exception when trying to register with RM !!", e);
	        System.exit(-1);
		}
		// Register self with ResourceManager
		// Dump out information about cluster capability as seen by the resource
		// manager
		int maxMem = response.getMaximumResourceCapability().getMemory();
		
		logger.info("Max mem capability of resources in this cluster " + maxMem);

		// A resource ask has to be atleast the minimum of the capability of the
		// cluster, the value has to be
		// a multiple of the min value and cannot exceed the max.
		// If it is not an exact multiple of min, the RM will allocate to the
		// nearest multiple of min
		if (containerMemory > maxMem) {
			logger.info("Container memory for Orca node specified above max threshold of YARN cluster. Using max value."
					+ ", specified=" + containerMemory + ", max=" + maxMem);
			containerMemory = maxMem;
		}
		
		while(true && (!tp.isShutdown())) {
			ControllerRequest req = null;
			try {
				req = oc.getNextRequest();
			} catch (InterruptedException e1) {
				logger.info("Got InterruptException !!" );
			}
			logger.info("Got Request from OrcaController[" + req.getType() + "]" );
			if ((req != null) && req.getType().equals(ReqType.CONTAINER)) {
				requester.inQ.add(req);
			} else {
				handleLaunchRequest(req);
			}

		}
		
	}

	private void handleLaunchRequest(ControllerRequest req) {
		OrcaLaunchContext launchContext = req.getLaunchContext();
		ContainerNode requestNode = (ContainerNode)req.getRequestNode();
		tp.submit(new LaunchContainerRunnable(launchContext, requestNode.container));
	}
	
	/**
	 * Ask RM to allocate given no. of containers to this Application Master
	 * 
	 * @param requestedContainers
	 *            Containers to ask for from RM
	 * @return Response from RM to AM with allocated containers
	 * @throws IOException 
	 * @throws YarnException 
	 * @throws YarnRemoteException
	 */
	private AllocateResponse sendContainerAskToRM(List<ContainerRequest> requestedContainers) throws YarnException, IOException {
	    logger.info("Sending request to RM for containers [" + requestedContainers.size() + "]");

	    for (ContainerRequest cr : requestedContainers) {
	      resourceManager.addContainerRequest(cr);
	    }
	    return resourceManager.allocate(50);
	}	
	
	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @param numContainers
	 *            Containers to ask for from RM
	 * @return the setup ResourceRequest to be sent to RM
	 */
	private List<ContainerRequest> setupContainerAskForRM(int numContainers, List<NodeReport> nodeReport) {
      // set the priority for the request
	  List<ContainerRequest> retList = new ArrayList<ContainerRequest>();
	  
//	  if (nodeReport == null) {
//	    nodeReport = new ArrayList<NodeReport>();
//	  }
//	  int numNodes = nodeReport.size();
//	  int startNode = -1;
//	  if (numNodes > 0) {
//	    startNode = rnd.nextInt(numNodes);
//	  }
	  
	  for (int i = 0; i < numContainers; i++) {
	      Priority pri = Priority.newInstance(0);
	      Resource capability = Resource.newInstance(containerMemory, 1);
//	      String[] host = (numNodes > 0) ? new String[] {nodeReport.get(startNode).getNodeId().getHost()} : null; 
	      ContainerRequest contReq = new ContainerRequest(capability, null, null, pri);
	      retList.add(contReq);
//	      startNode = (startNode + 1) % numNodes;
	  }
	  return retList;
	}		
	
	/**
	 * Connect to the Resource Manager
	 * 
	 * @return Handle to communicate with the RM
	 */
	private AMRMClient<ContainerRequest> connectToRM() {
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    InetSocketAddress rmAddress = yarnConf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
	            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
	    logger.info("Connecting to ResourceManager at " + rmAddress);
	    AMRMClient<ContainerRequest> amRMClient = AMRMClient.createAMRMClient();
	    amRMClient.setNMTokenCache(NMTokenCache.getSingleton());
	    amRMClient.init(yarnConf);
	    amRMClient.start();
	    return amRMClient;
//	    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
	}	
	
	/**
	 * Register the Application Master to the Resource Manager
	 * 
	 * @return the registration response from the RM
	 * @throws IOException 
	 * @throws YarnException 
	 * @throws UnknownHostException 
	 * @throws YarnRemoteException
	 */
	private RegisterApplicationMasterResponse registerToRM() throws UnknownHostException, YarnException, IOException {
	    RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);

	    // set the required info into the registration request:
	    // application attempt id,
	    // host on which the app master is running
	    // rpc port on which the app master accepts requests from the client
	    // tracking url for the app master
//	    appMasterRequest.setApplicationAttemptId(appAttemptID);
	    appMasterRequest.setHost(appMasterHostname);
	    appMasterRequest.setRpcPort(appMasterRpcPort);
	    try {
			appMasterRequest.setTrackingUrl(trackingService.getTrackingUrl());
		} catch (UnknownHostException e) {
			logger.error("Got error retriving hostname !!", e);
		}

	    return resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, trackingService.getTrackingUrl());
	}
	
	private void killApp() throws YarnException, IOException {
	    // When the application completes, it should send a finish application signal
	    // to the RM
	    logger.info("Application completed. Signalling finish to RM");

	    nmClient.stop();
	    resourceManager.unregisterApplicationMaster(FinalApplicationStatus.KILLED, "APP Ended", null);
	}

	@Override
	public void nodeUp(HeartbeatNode node, NodeState firstNodeState) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nodeWarn(HeartbeatNode node, NodeState lastNodeState) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
		// TODO Auto-generated method stub
		
	}

	
	public static class ContainerNode implements Node {

		public final Container container;
		
		public ContainerNode(Container container) {
			this.container = container;
		}
		
		@Override
		public int getId() {
			return container.getId().getId();
		}
		
	}
	
	private class RequestContainerRunnable implements Runnable {
		private final LinkedBlockingQueue<ControllerRequest> inQ = new LinkedBlockingQueue<ControllerRequest>();
		private int exCount = 0;
		@Override
		
		public void run() {
			while (true) {
			  String proxyStats = null;
			  int lastMax = 0;
			  try {
			    proxyStats = OrcaAppMaster.this.oc.getProxyStats();
			    if (proxyStats != null) {
			      logger.info("Got container stats <" + proxyStats + ">!!");
			    }
			    JSONArray containerStats = new JSONArray(proxyStats);
			    if (containerStats != null) {
			      for (int i = 0; i < containerStats.length(); i ++) {
			        JSONObject stat = (JSONObject)containerStats.get(i);
			        int inProgress = Integer.parseInt((String)stat.get("outstanding"));
			        if ((inProgress > 10)&&(inProgress > lastMax)) {
			          oc.scaleUpBy(1);
			          lastMax = (inProgress > lastMax) ? inProgress : lastMax;
			          break;
			        }
			      }
			    }
			  } catch (Exception e) {
			    logger.info("Got error while getting container stats <" + proxyStats + ">!!", e);
			  }
			  if (inQ.size() != 0) {
			    LinkedList<ControllerRequest> outstanding = new LinkedList<ControllerRequest>();
			    inQ.drainTo(outstanding);
			    List<ContainerRequest> containerReqs = OrcaAppMaster.this.setupContainerAskForRM(outstanding.size(), null);
			    
			    // Send request to RM
			    logger.info("Asking RM for a container !!");
			    // Retrieve list of allocated containers from the response
			    AllocateResponse allocResp = null;
			    try {
			      allocResp = sendContainerAskToRM(containerReqs);
			    } catch (Exception e) {
			      logger.info("Got Error response when sending containerAsk !!", allocResp);
			    }
			    logger.info("Got response from RM for container ask, allocatedCnt=" + allocResp.getAllocatedContainers().size());
			    for (Container allocatedContainer : allocResp.getAllocatedContainers()) {
			      ControllerRequest req = outstanding.poll();
			      if (req != null) {
			        req.setResponse(new ContainerNode(allocatedContainer));	
			      }
			    }
			    List<ContainerStatus> completedContainersStatuses = allocResp.getCompletedContainersStatuses();
			    for (ContainerStatus compContStat : completedContainersStatuses) {
			      releasedContainers.add(compContStat.getContainerId());
			    }
			    while (outstanding.size() > 0) {
			      inQ.add(outstanding.poll());
			    }
			  }
			  try {
			    Thread.sleep(1000);
			  } catch (InterruptedException e) {
			    // Don't care..
			  }
			}
			
		}
	}	
	
	private class LaunchContainerRunnable implements Runnable {

		final OrcaLaunchContext launchContext;
	    // Allocated container
	    final Container container;

	    /**
	     * @param lcontainer
	     *            Allocated container
	     */
	    public LaunchContainerRunnable(OrcaLaunchContext launchContext, Container lcontainer) {
	        this.container = lcontainer;
	        this.launchContext = launchContext;
	    }
	    
	    /**
	     * Helper function to connect to CM
	     */
//	    private void connectToCM() {
//	      logger.debug("Connecting to ContainerManager for containerid=" + container.getId());
//	        NMClient nmClient = NMClient.createNMClient();
//	        nmClient.init(conf);
//	        container.
//	        String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
//	        InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
//	        logger.info("Connecting to ContainerManager at " + cmIpPortStr);
//	        this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf));
//	    }
	    


		@Override
		public void run() {

	        logger.info("Setting up container launch container for containerid=" + container.getId());
	        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
	        // Set the local resources
	        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	        
	        try {
	            FileSystem fs = FileSystem.get(conf);

	            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(fs.getHomeDirectory(), "/app-"
	                    + appAttemptID.getApplicationId().getId()), false);
	            while (files.hasNext()) {
	                LocatedFileStatus file = files.next();
	                LocalResource localResource = Records.newRecord(LocalResource.class);

	                localResource.setType(LocalResourceType.FILE);
	                localResource.setVisibility(LocalResourceVisibility.APPLICATION);
	                localResource.setResource(ConverterUtils.getYarnUrlFromPath(file.getPath()));
	                localResource.setTimestamp(file.getModificationTime());
	                localResource.setSize(file.getLen());
	                localResources.put(file.getPath().getName(), localResource);
	            }
	            ctx.setLocalResources(localResources);

	        } catch (IOException e1) {
	            logger.error("Got error when setting LocalResource !!", e1);
	        }

	        StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");

	        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
	                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
	            classPathEnv.append(':');
	            classPathEnv.append(c.trim());
	        }

	        // classPathEnv.append(System.getProperty("java.class.path"));
	        Map<String, String> env = launchContext.getEnv();

	        env.put("CLASSPATH", classPathEnv.toString());
	        ctx.setEnvironment(env);

	        List<String> commands = new ArrayList<String>();
	        commands.add(launchContext.getShellCommand());
	        ctx.setCommands(commands);

	        try {
	          nmClient.startContainer(container, ctx);
	        } catch (Exception e) {
	          logger.error("Got exception when trying to start Container !!", e);
	        }
		}
	}
	
	public class TrackingService extends AbstractHandler implements Runnable {
		
		private int runningPort = -1;
		private Server jettyServer = null;
		private boolean hasStarted = false;

		@Override
		public void run() {
//			WebAppContext webApp = new WebAppContext();
//			webApp.setContextPath("/orca_app");
//			webApp.setHandler(this);
			
			// Let Jetty start on any available port
			this.jettyServer = new Server(0);
			this.jettyServer.setHandler(this);
			try {
				this.jettyServer.start();
				this.hasStarted = true;
				this.runningPort = (this.jettyServer.getConnectors()[0]).getLocalPort();								
//				OrcaAppMaster.this.appMasterTrackingUrl = 
//						"http://" + InetAddress.getLocalHost().getHostName() + ":" + this.runningPort 
//						+ "/orca_app/status";
				logger.info("\n\nStarting Tracking Service on port [" + this.runningPort + "]\n\n");
			} catch (Exception e) {
				logger.error("Could not start Web Container !!", e);
				System.exit(-1);
			}
			
			try {
				this.jettyServer.join();
			} catch (InterruptedException e) {
				logger.error("Web Container interrupted!!", e);
			}
			
		}
		
		public int getRunningPort() {
			return runningPort;
		}
		
		public boolean isRunning() {
			return hasStarted;
		}
		
		public String getTrackingUrl() throws UnknownHostException {
			return "http://" + InetAddress.getLocalHost().getHostName() + ":" + this.runningPort 
			+ "/orca_app/status";
		}

		@Override
		public void handle(String target, HttpServletRequest request,
				HttpServletResponse response, int dispatch) throws IOException,
				ServletException {
	        response.setContentType("text/json;charset=utf-8");
	        response.setStatus(HttpServletResponse.SC_OK);

	        Request base_request = (request instanceof Request) ? (Request)request : HttpConnection.getCurrentConnection().getRequest();
	        base_request.setHandled(true);
	        
	        JSONObject jResp = new JSONObject();
	        try {
	        	if (target.endsWith("status")) {
	        		jResp.put("num_containers_running", OrcaAppMaster.this.oc.getNumRunningContainers());
					jResp.put("num_containers_died", OrcaAppMaster.this.oc.getNumContainersDied());
					jResp.put("proxy_url", OrcaAppMaster.this.oc.getProxyURL());
					jResp.put("container_info", OrcaAppMaster.this.oc.getContainerInfo());
					jResp.put("container_stats", new JSONArray(OrcaAppMaster.this.oc.getProxyStats()));
	        	} else if (target.endsWith("kill")) {
	        		OrcaAppMaster.this.oc.kill();
	        		Thread.sleep(5000);
	        		OrcaAppMaster.this.killApp();
	        		OrcaAppMaster.this.tp.shutdownNow();
	        		logger.info("\nRecieved Request to Kill self !!\n");
	        		jResp.put("status", "axed");
	        	} else if (target.endsWith("scale_up")) {
	        		OrcaAppMaster.this.oc.scaleUpBy(1);
	        		jResp.put("status", "running");
	        		jResp.put("scale_up", "1");
	        	}
	        } catch (Exception e) {
	        	// Ignore for the time being..
	        }
	        
	        response.getWriter().println(jResp.toString());
			
		}
		
	}

}
