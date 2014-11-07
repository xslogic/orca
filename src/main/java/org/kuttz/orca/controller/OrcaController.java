package org.kuttz.orca.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.kuttz.orca.hmon.HBMasterArgs;
import org.kuttz.orca.hmon.HeartbeatMaster;
import org.kuttz.orca.hmon.HeartbeatMasterClient;
import org.kuttz.orca.hmon.HeartbeatNode;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.kuttz.orca.hmon.NodeType;
import org.kuttz.orca.hmon.THeartbeatCommandEndPoint;
import org.kuttz.orca.proxy.ELBArgs;
import org.kuttz.orca.proxy.TProxyCommandEndPoint;
import org.kuttz.orca.proxy.TProxyCommandEndPoint.Client;
import org.kuttz.orca.web.WebAppArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaController implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(OrcaController.class);
	
	private LinkedBlockingQueue<ClientRequest> inQ = new LinkedBlockingQueue<ClientRequest>();
	private LinkedBlockingQueue<ControllerRequest> outQ = new LinkedBlockingQueue<ControllerRequest>();
	
	private final OrcaControllerArgs ocArgs;
	
	private final OrcaControllerClient client;
	
	private volatile HeartbeatMaster hbMaster = null;	
	
	private volatile AtomicInteger numContainersRunning = new AtomicInteger(0);
	
	private volatile AtomicInteger numContainersDied = new AtomicInteger(0);
	
	private List<SlaveHandler> slaves = new ArrayList<OrcaController.SlaveHandler>();
	private ELBSlaveHandler elbHandler;
	
	public OrcaController(OrcaControllerArgs args, OrcaControllerClient client) {
		this.ocArgs = args;
		this.client = client;
	}
	
	/**
	 * Interface to the outside world. An external client
	 * such as the ApplicationMaster will wait on this for any
	 * new requests from the Controller
	 * @return
	 * @throws InterruptedException 
	 */
	public ControllerRequest getNextRequest() throws InterruptedException {
		return this.outQ.take();
	}
	
	public String getProxyURL() {
	  if (elbHandler != null) {
	    if (elbHandler.state.equals(SlaveState.NODE_RUNNING)) {
	      return "http://" + elbHandler.elbHost + ":" + elbHandler.elbPort + "/" + ocArgs.appName;
	    }
	  }
	  return "";
	}
	
	public List<String> getContainerInfo() {
	  List<String> retList = new ArrayList<String>();
	  for (SlaveHandler slave : slaves) {
	    if (slave instanceof ContainerSlaveHandler) {
	      retList.add(slave.sHost + ":" + ((ContainerSlaveHandler)slave).getRunningPort());
	    }
	  }
	  return retList;
	}
	
	public String getProxyStats() {
	  if (elbHandler != null) {
	    if (elbHandler.state.equals(SlaveState.NODE_RUNNING)) {
	      if (elbHandler.slaveNode != null) {
	        NodeState nodeState = hbMaster.getNodeState(elbHandler.slaveNode.getId(), NodeType.PROXY);
	        if ((nodeState != null) && (nodeState.nodeInfo != null)) {
	          return nodeState.nodeInfo.getInfoJson();
	        }
	      }
	    }
	  }
	  return "";
	}
	
	public int getProxyPort() {
		if (elbHandler != null) {
			if (elbHandler.state.equals(SlaveState.NODE_RUNNING)) {
				return elbHandler.elbPort;
			}
		}
		return -1;
	}
	
	public String getProxyHost() {
		if (elbHandler != null) {
			if (elbHandler.state.equals(SlaveState.NODE_RUNNING)) {
				return elbHandler.elbHost;
			}
		}
		return "";
	}
	
	
	public void scaleUpBy(int numContainers) {
		this.inQ.add(new ClientRequest(numContainers));
	}
	
	public void scaleDownBy(int numContainers) {
		this.inQ.add(new ClientRequest(-1 * numContainers));
	}	
	
	public void init() {
		try {
			this.hbMaster = startHbMaster(client);
		} catch (InterruptedException e) {
			logger.error("Thread interrupted", e);
		}
		this.elbHandler = new ELBSlaveHandler(createELBLaunchArgs(this.ocArgs));
		slaves.add(elbHandler);
		for (int i = 0; i < this.ocArgs.numContainers; i++) {
			slaves.add(new ContainerSlaveHandler(createContainerLaunchArgs(this.ocArgs)));
		}		
	}

	@Override
	public void run() {		
				
		while (true) {
			
			// Let ELB start first
			if (this.elbHandler.getSlaveState().equals(SlaveState.NOT_STARTED)) {
				this.elbHandler.askForNode();
			}
			
			// Once ELB is running, start other nodes
			if (this.elbHandler.getSlaveState().equals(SlaveState.NODE_RUNNING)) {
				for (SlaveHandler slave : slaves) {
					if (slave.getSlaveState().equals(SlaveState.NOT_STARTED)) {
						slave.askForNode();
					}
				}
			}
			
			ClientRequest req = null;
			try {
				req = this.inQ.poll(5000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// Dont Care
			}
			if (req != null) {
				handleClientRequest(req);
			}
		}		
	}
	
	private void handleClientRequest(ClientRequest req) {
		if (req.scaleNum > 0) {
			for (int i = 0; i < req.scaleNum; i++) {
				slaves.add(new ContainerSlaveHandler(createContainerLaunchArgs(this.ocArgs)));
			}
		}
	}

	private WebAppArgs createContainerLaunchArgs(OrcaControllerArgs ocArgs) {
		WebAppArgs containerArgs = new WebAppArgs();
		containerArgs.appName = ocArgs.appName;
		containerArgs.warLocation = ocArgs.warLocation;
		return containerArgs;
	}

	private ELBArgs createELBLaunchArgs(OrcaControllerArgs ocArgs) {
		ELBArgs elbArgs = new ELBArgs();
		elbArgs.maxPort = ocArgs.elbMaxPort;
		elbArgs.minPort = ocArgs.elbMinPort;
		return elbArgs;
	}

	public HeartbeatMaster getHeartbeatMaster() {
		return this.hbMaster;
	}
	
	public int getNumRunningContainers() {
		return this.numContainersRunning.get();
	}
	
	public int getNumContainersDied() {
		return this.numContainersDied.get();
	}
	
	public void kill() {
		for (SlaveHandler slave : slaves) {
			slave.kill();
		}
	}
	
	private HeartbeatMaster startHbMaster(OrcaControllerClient client) throws InterruptedException {
		HBMasterArgs hbMasterArgs = new HBMasterArgs();
		hbMasterArgs.checkPeriod = this.ocArgs.hbPeriod;
		hbMasterArgs.deadTime = this.ocArgs.hbDeadTime;
		hbMasterArgs.warnTime = this.ocArgs.hbWarnTime;
		hbMasterArgs.minPort = this.ocArgs.hbMinPort;
		hbMasterArgs.maxPort = this.ocArgs.hbMaxPort;
		hbMasterArgs.numCheckerThreads = this.ocArgs.hbCheckerThreads;
		hbMasterArgs.numHbThreads = this.ocArgs.hbMasterThreads;
		
		HeartbeatMaster hbMaster = new HeartbeatMaster(hbMasterArgs);
		hbMaster.init();
		hbMaster.registerClient(client.getHeartbeatClient());
		client.getExecutorService().submit(hbMaster);
		while (!hbMaster.isRunning()) {
			logger.debug("Waiting for HBMaster to start..");
			Thread.sleep(1000);
		}
		return hbMaster;
	}
	
	/**
	 * Placeholder class 
	 * @author suresa1
	 *
	 */
	public static class ClientRequest {
		
		public final int scaleNum;
		
		public ClientRequest(int scaleNum) {
			this.scaleNum = scaleNum;
		}
		
	}
	
	public static class ControllerRequest {
		
		public static enum ReqType {
			CONTAINER, LAUNCH;
		} 
		
		private final ReqType type;
		private volatile Node response;
		private volatile Node requestNode;  
		private OrcaLaunchContext launchContext;
		private SlaveHandler handler;
		
		public ControllerRequest(SlaveHandler handler) {
			this.type = ReqType.CONTAINER;
			this.handler = handler;
		}
		
		public ControllerRequest(OrcaLaunchContext lc, Node node) {
			this.type = ReqType.LAUNCH;
			this.launchContext = lc;
			this.requestNode = node;
		}		
		
		public ReqType getType() { return type; }
		
		public Node getRequestNode() {
			if (this.type.equals(ReqType.CONTAINER)) {
				return null;
			}
			return requestNode;
		}
		
		// Has to be called by AM
		public void setResponse(Node nodeResp) {
			this.response = response;
			this.handler.lease(nodeResp);
		}
		
		public OrcaLaunchContext getLaunchContext() {
			return launchContext;
		}
		
	}
	
	public static interface Node {
		public int getId();
	}
		
	public static enum SlaveState { 
		NOT_STARTED, NODE_REQUESTED, NODE_LEASED, NODE_LAUNCHED, NODE_RUNNING; 
	}	
	
	// Need to create a slaveHandler per container (proxy/webContainer).. and register with controller
	public abstract class SlaveHandler implements HeartbeatMasterClient {		
		
		protected volatile Node slaveNode = null;
		private final NodeType nType;
		private final Object launchArgs;
		
		private String sHost;
		private int sPort;
		
		protected volatile SlaveState state = SlaveState.NOT_STARTED;
		
		public SlaveHandler(NodeType nType, Object launchArgs) {
			this.nType = nType;
			this.launchArgs = launchArgs;
		}
		
		private void launchNode(Node preInitNode) {
			OrcaLaunchContext lContext = null;
			if (NodeType.CONTAINER.equals(nType)) {
				lContext = createContainerLaunchContext(preInitNode.getId(), (WebAppArgs)launchArgs);
			} else {
				lContext = createProxyLaunchContext(preInitNode.getId(), (ELBArgs)launchArgs);
			}
			ControllerRequest lRequest = new ControllerRequest(lContext, preInitNode);
			// Register for heartbeat for this node
			OrcaController.this.hbMaster.registerClient(preInitNode.getId(), nType, this);
			OrcaController.this.outQ.add(lRequest);
			this.state = SlaveState.NODE_LAUNCHED;
		}
		
		public void askForNode() {
			ControllerRequest req = new ControllerRequest(this);
			OrcaController.this.outQ.add(req);
			this.state = SlaveState.NODE_REQUESTED;
		}
		
		public void lease(Node node) {
			this.slaveNode = node;
			this.state = SlaveState.NODE_LEASED;
			launchNode(slaveNode);
		}
		
		public SlaveState getSlaveState() {
			return this.state;
		}
		
		public void kill() {
			TTransport transport = new TFramedTransport(new TSocket(sHost, sPort));
			try {
				transport.open();
				TBinaryProtocol protocol = new TBinaryProtocol(transport);
				THeartbeatCommandEndPoint.Client cl = new THeartbeatCommandEndPoint.Client(protocol);
				cl.killSelf();
			} catch (Exception e) {
				logger.error("Could not send Kill signal !!", e);
			}
		}
		
		/** Heartbeat Callback */		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			if (nType.equals(NodeType.CONTAINER)) {
				OrcaController.this.numContainersRunning.incrementAndGet();
			}
			this.sHost = nodeState.host;
			this.sPort = nodeState.port;
			this.state = SlaveState.NODE_RUNNING;
		}
		
		@Override
		public void nodeWarn(HeartbeatNode node, NodeState nodeState) {
			// TODO Auto-generated method stub			
		}
		
		@Override
		public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
			OrcaController.this.numContainersDied.incrementAndGet();
			OrcaController.this.numContainersRunning.decrementAndGet();
			this.slaveNode = null;
			this.state = SlaveState.NOT_STARTED;
		}
		
		private OrcaLaunchContext createProxyLaunchContext(int nodeId, ELBArgs elbArgs) {
			LinkedList<String> vargs = new LinkedList<String>();
			HashMap<String,String> env = new HashMap<String, String>();				
			
	        vargs.add("java");

	        //vargs.add("-Xmx" + containerMemory + "m");
	 		//vargs.add("-Xdebug");
	 		//vargs.add("-Xrunjdwp:transport=dt_socket,address=8889,server=y,suspend=n");	        
	        vargs.add("org.kuttz.orca.OrcaDaemon");
	        
	        vargs.add("-proxy");
	        vargs.add("true");
	        vargs.add("-elb_min_port");
	        vargs.add("" + OrcaController.this.ocArgs.elbMinPort);
	        vargs.add("-elb_max_port");
	        vargs.add("" + OrcaController.this.ocArgs.elbMaxPort);
	        addHBSlaveArgs(nodeId, vargs);
	        vargs.add("1>/tmp/orca-proxy-stdout" + nodeId);
	        vargs.add("2>/tmp/orca-proxy-stderr" + nodeId);
			
	        StringBuilder command = new StringBuilder();
	        for (CharSequence str : vargs) {
	            command.append(str).append(" ");
	        }		
			return new OrcaLaunchContext(command.toString(), env);
		}
				
		
		private OrcaLaunchContext createContainerLaunchContext(int nodeId, WebAppArgs containerArgs) {
			LinkedList<String> vargs = new LinkedList<String>();
			HashMap<String,String> env = new HashMap<String, String>();				
			
	        vargs.add("java");

	        //vargs.add("-Xmx" + containerMemory + "m");
	 		//vargs.add("-Xdebug");
	 		//vargs.add("-Xrunjdwp:transport=dt_socket,address=8889,server=y,suspend=n");	        
	        vargs.add("org.kuttz.orca.OrcaDaemon");
	        
	        vargs.add("-container");
	        vargs.add("true");	        
	        vargs.add("-app_name");
	        vargs.add("" + OrcaController.this.ocArgs.appName);
	        vargs.add("-war_location");
	        vargs.add("" + OrcaController.this.ocArgs.warLocation);
	        addHBSlaveArgs(nodeId, vargs);
	        vargs.add("1>/tmp/orca-container-stdout" + nodeId);
	        vargs.add("2>/tmp/orca-container-stderr" + nodeId);
			
	        StringBuilder command = new StringBuilder();
	        for (CharSequence str : vargs) {
	            command.append(str).append(" ");
	        }		
			return new OrcaLaunchContext(command.toString(), env);		
		}
		
		private void addHBSlaveArgs(int nodeId, LinkedList<String> vargs) {
			vargs.add("-send_period");
			vargs.add("" + OrcaController.this.ocArgs.hbPeriod);
			vargs.add("-master_host");
			try {
				vargs.add(InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				logger.error("Exception while extracting hostname !!", e);
			}			
			vargs.add("-master_port");
			vargs.add("" + OrcaController.this.hbMaster.getRunningPort());
			vargs.add("-send_timeout");
			vargs.add("" + (OrcaController.this.ocArgs.hbPeriod / 2));
			vargs.add("-node_id");
			vargs.add("" + nodeId);
			vargs.add("-num_sender_threads");
			vargs.add("1");
			vargs.add("-hb_min_port");
			vargs.add("" + OrcaController.this.ocArgs.hbMinPort);
			vargs.add("-hb_max_port");
			vargs.add("" + OrcaController.this.ocArgs.hbMaxPort);
		}
	}
	
	public class ELBSlaveHandler extends SlaveHandler {
		
		// Host & Port to forward requests to containers
		private String elbHost;
		private int elbPort = -1;
		
		// Port to send commands to ELB (eg. add/remove node etc.) 
		private int commandPort = -1;		
		
		public ELBSlaveHandler(ELBArgs launchArgs) {
			super(NodeType.PROXY, launchArgs);			
		}
		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			if ((node.getId() == this.slaveNode.getId()) 
					&& (!this.state.equals(SlaveState.NODE_RUNNING))) {
				super.nodeUp(node, nodeState);
				this.elbHost = nodeState.host;
				this.elbPort = nodeState.nodeInfo.getAuxEndPointPort1();
				this.commandPort = nodeState.nodeInfo.getAuxEndPointPort2();
				// Notify ELB of any new nodes going up/down
				Map<HeartbeatNode, NodeState> existingNodes = OrcaController.this.hbMaster.registerClient(this);
				for (Entry<HeartbeatNode, NodeState> e : existingNodes.entrySet()) {
					try {
						if (e.getKey().getId() != this.slaveNode.getId()) {
							logger.info("ELB Recieved Existing NODE up from [" 
									+ e.getValue().host + ", " 
									+ e.getValue().nodeInfo.getAuxEndPointPort1() + "]");
							addNode(e.getValue().host, e.getValue().nodeInfo.getAuxEndPointPort1(), 1);
						}
					} catch (IOException ex) {
						logger.error("ELB SlaveHandler could not add Node [" + e.getValue().host + ", " + e.getValue().nodeInfo.getAuxEndPointPort1() + "]", e);
					}					
				}
			} else {
				try {
					// TODO : handle weights
					logger.info("ELB Recieved NODE up from [" 
							+ nodeState.host + ", " 
							+ nodeState.nodeInfo.getAuxEndPointPort1() +  "]");
					addNode(nodeState.host, nodeState.nodeInfo.getAuxEndPointPort1(), 1);
				} catch (IOException e) {
					logger.error("ELB SlaveHandler could not add Node [" + nodeState.host + ", " + nodeState.nodeInfo.getAuxEndPointPort1() + "]", e);
				}
			}
		}
		
		@Override
		public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
			if (node.getId() != this.slaveNode.getId()) {
				try {
					removeNode(lastNodeState.host, lastNodeState.nodeInfo.getAuxEndPointPort1());
				} catch (IOException e) {
					logger.error("ELB SlaveHandler could not add Node [" + lastNodeState.host + ", " + lastNodeState.nodeInfo.getAuxEndPointPort1() + "]");
				}				
			} else {
				OrcaController.this.numContainersDied.incrementAndGet();
				this.state = SlaveState.NOT_STARTED;				
			}
		}

		private TProxyCommandEndPoint.Client createClient() {
			TTransport transport = new TFramedTransport(new TSocket(elbHost, commandPort));
			try {
				transport.open();
				TBinaryProtocol protocol = new TBinaryProtocol(transport);
				return new TProxyCommandEndPoint.Client(protocol);				
			} catch (TTransportException e) {
				logger.error("Could not create client to Elb Command EndPoint", e);
				return null;
			}
		}		
		
		public boolean addNode(String host, int port, int weight) throws IOException {
			if (state.equals(SlaveState.NODE_RUNNING)) {
				Client client = createClient();
				try {
					return client.addNode(host, port, weight);
				} catch (TException e) {
					throw new IOException("Could not contact Elb Command EndPoint", e);
				}
			} else {
				return false;
			}
		}
		
		public boolean removeNode(String host, int port) throws IOException {
			Client client = createClient();			
			try {
				return client.removeNode(host, port);
			} catch (TException e) {
				throw new IOException("Could not contact Elb Command EndPoint", e);
			}			
		}
		
		public int getElbPort() {
			return this.elbPort;
		}
		
		public String getElbHost() {
			return this.elbHost;
		}
	}
	
	public class ContainerSlaveHandler extends SlaveHandler {

		private int runningPort = -1;
		
		public ContainerSlaveHandler(WebAppArgs launchArgs) {
			super(NodeType.CONTAINER, launchArgs);
		}
		
		@Override
		public void nodeUp(HeartbeatNode node, NodeState nodeState) {
			super.nodeUp(node, nodeState);
			this.runningPort = nodeState.nodeInfo.getAuxEndPointPort1();
		}
		
		public int getRunningPort() {
			return this.runningPort;
		}
	}	
	
}
