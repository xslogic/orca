package org.kuttz.orca.hmon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatMaster implements THeartbeatEndPoint.Iface, Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(HeartbeatMaster.class);
	
	private final ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>> nodeRegistry = 
			new ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>>();
	private ScheduledExecutorService schedExService = null;
	
	private ConcurrentHashMap<HeartbeatNode, Set<HeartbeatMasterClient>> nodeClients = new ConcurrentHashMap<HeartbeatNode, Set<HeartbeatMasterClient>>();
	
	private HBMasterArgs masterArgs;
	
	private volatile boolean hasStarted = false;
	
	private volatile int runningPort;
	
	public HeartbeatMaster(HBMasterArgs args) {
		this.masterArgs = args;
	}	
	
	public Map<HeartbeatNode, NodeState> registerClient(HeartbeatMasterClient client) {
		registerClient(-1, NodeType.CONTAINER, client);
		HashMap<HeartbeatNode,NodeState> retVal = new HashMap<HeartbeatNode, NodeState>();
		for (Entry<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>> e : nodeRegistry.entrySet()) {
			ConcurrentHashMap<HeartbeatNode,NodeState> m = e.getValue();
			for (Entry<HeartbeatNode,NodeState> e2 : m.entrySet()) {
				retVal.put(e2.getKey(), e2.getValue());
			}
		}
		return retVal;
	}	
	
	public void registerClient(int nodeId, NodeType nodeType, HeartbeatMasterClient client) {
		Set<HeartbeatMasterClient> sTemp = new HashSet<HeartbeatMasterClient>();
		Set<HeartbeatMasterClient> sClients = nodeClients.putIfAbsent(new HeartbeatNode(nodeId, nodeType), sTemp);
		if (sClients == null) {
			sClients = sTemp;
		}
		sClients.add(client);
	}
	
	public NodeState getNodeState(int nodeId, NodeType nType) {
		ConcurrentHashMap<HeartbeatNode,NodeState> m1 = nodeRegistry.get(nType);
		if (m1 != null) {
			return m1.get(new HeartbeatNode(nodeId, nType));
		}
		return null;
	}
	
	public void init() {
		this.schedExService = new ScheduledThreadPoolExecutor(masterArgs.numCheckerThreads);
	}
	
	@Override
	public void run() {
		// Start timer
		schedExService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long cTime = System.currentTimeMillis();
				List<HeartbeatNode> nodesToDelete = new ArrayList<HeartbeatNode>();
				List<HeartbeatNode> nodesToWarn = new ArrayList<HeartbeatNode>();
				for (Entry<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>> e : nodeRegistry.entrySet()) {
					NodeType nType = e.getKey();					
					ConcurrentHashMap<HeartbeatNode,NodeState> m = e.getValue();
					for (Entry<HeartbeatNode,NodeState> e2 : m.entrySet()) {
						HeartbeatNode node = e2.getKey();
						long tDiff = cTime - e2.getValue().timeStamp.get();
						logger.debug("Found Node [" + nType + ", " + node.getId() + "]");
						if (tDiff > masterArgs.warnTime) {
							logger.debug("Node hasnt sent Heartbeats for a while [" + nType + ", " + node.getId() + "]");
							if (tDiff > masterArgs.deadTime) {
								nodesToDelete.add(node);
								logger.debug("Node deemed dead [" + nType + ", " + node.getId() + "]");
							} else {
								nodesToWarn.add(node);
							}							
						}						
					}
				}
				for (HeartbeatNode node : nodesToDelete) {
					NodeState nodeState = nodeRegistry.get(node.getType()).get(node);					
					Set<HeartbeatMasterClient> s1 = nodeClients.get(new HeartbeatNode(-1, NodeType.CONTAINER));
					if (s1 != null) {
						for (HeartbeatMasterClient c : s1) {
							c.nodeDead(node, nodeState);
						}
					}
					Set<HeartbeatMasterClient> s2 = nodeClients.get(node);
					if (s2 != null) {
						for (HeartbeatMasterClient c : s2) {
							c.nodeDead(node, nodeState);
						}
					}
					nodeRegistry.get(node.getType()).remove(node);
				}
				for (HeartbeatNode node : nodesToWarn) {
					NodeState nodeState = nodeRegistry.get(node.getType()).get(node);					
					Set<HeartbeatMasterClient> s1 = nodeClients.get(new HeartbeatNode(-1, NodeType.CONTAINER));
					if (s1 != null) {
						for (HeartbeatMasterClient c : s1) {
							c.nodeWarn(node, nodeState);
						}
					}
					Set<HeartbeatMasterClient> s2 = nodeClients.get(node);
					if (s2 != null) {
						for (HeartbeatMasterClient c : s2) {
							c.nodeWarn(node, nodeState);
						}
					}
				}				
				
			}			
		}, masterArgs.checkPeriod, masterArgs.checkPeriod, TimeUnit.MILLISECONDS);
		
		try {
			TNonblockingServerTransport trans = createTransport(masterArgs.minPort, masterArgs.maxPort);
            THsHaServer.Args args = new THsHaServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.processor(new THeartbeatEndPoint.Processor<HeartbeatMaster>(this));
            args.workerThreads(masterArgs.numHbThreads);
			THsHaServer server = new THsHaServer(args);
			server.serve();
		} catch (Exception e) {
			logger.error("Could not start Heartbeat Master", e);
		}		
		
	}
		
	private TNonblockingServerTransport createTransport(int minPort, int maxPort) throws IOException {
		for (int p = minPort; p <= maxPort; p++) {
			try {				
				TNonblockingServerSocket t = new TNonblockingServerSocket(p);
				logger.info("Starting Heartbeat Master on port [" + p + "]");
				this.hasStarted = true;
				this.runningPort = p;
				return t;
			} catch (Exception e) {
				logger.info("Could not create server on port [" + p + "] !!");
				continue;
			}
		}
		
		throw new IOException("No free ports available from [" + minPort + " to " + maxPort + "]");
	}
	
	@Override
	public boolean acceptHeartbeat(HeartbeatMsg hbMsg) throws TException {
		NodeType nodeType = hbMsg.getNodeType();
		int nodeId = hbMsg.getNodeId();
		logger.debug("Received Heartbeat from node [" + nodeType + ", " + nodeId + "]");
		ConcurrentHashMap<HeartbeatNode,NodeState> mTemp = new ConcurrentHashMap<HeartbeatNode, NodeState>();
		ConcurrentHashMap<HeartbeatNode,NodeState> m = nodeRegistry.putIfAbsent(nodeType, mTemp);		
		if (m == null) {
			m = mTemp;
		}
		long cTime = System.currentTimeMillis();		
		NodeState nsTemp = new NodeState();
		HeartbeatNode hKey = new HeartbeatNode(nodeId, nodeType);		
		NodeState ns = m.putIfAbsent(hKey, nsTemp);
		boolean isFirst = false;
		if (ns == null) {
			ns = nsTemp;
			isFirst = true;
		}		
		ns.timeStamp.set(cTime);
		ns.host = hbMsg.getHost();
		ns.port = hbMsg.getCommandPort();			
		ns.nodeInfo = hbMsg.getNodeInfo();
		if (isFirst) {
			Set<HeartbeatMasterClient> s1 = nodeClients.get(new HeartbeatNode(-1, NodeType.CONTAINER));
			if (s1 != null) {
				for (HeartbeatMasterClient c : s1) {
					c.nodeUp(hKey, ns);
				}
			}			
			Set<HeartbeatMasterClient> cSet = nodeClients.get(hKey);			
			if (cSet != null) {
				for (HeartbeatMasterClient client : cSet) {
					client.nodeUp(hKey, ns);
				}
			}
		}
		return true;
	}
	
	public boolean isRunning() {
		return this.hasStarted;
	}
	
	public int getRunningPort() {
		return this.runningPort;
	}
	
	public static void main(String[] args) throws Exception {
		HBMasterArgs hbArgs = new HBMasterArgs();
		hbArgs.checkPeriod = 10000;
		hbArgs.deadTime = 30000;
		hbArgs.numCheckerThreads = 1;
		hbArgs.numHbThreads = 2;
		hbArgs.minPort = 8810;
		hbArgs.maxPort = 8850;
		hbArgs.warnTime = 15000;
		
		
		HeartbeatMaster heartbeatMaster = new HeartbeatMaster(hbArgs);
		heartbeatMaster.registerClient(new HeartbeatMasterClient() {
			@Override
			public void nodeWarn(HeartbeatNode node, NodeState nodeState) {
				System.out.println("Client warned of node inactivity !!");
			}
			@Override
			public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
				System.out.println("Client notified of node death !!");
			}
			@Override
			public void nodeUp(HeartbeatNode node, NodeState nState) {
				// TODO Auto-generated method stub
				
			}
		});
		heartbeatMaster.init();
		
		System.out.println("Has started 1 [" + heartbeatMaster.isRunning() + ", " + heartbeatMaster.getRunningPort() + "]");		
		new Thread(heartbeatMaster).start();
		Thread.sleep(2000);
		System.out.println("Has started 2 [" + heartbeatMaster.isRunning() + ", " + heartbeatMaster.getRunningPort() + "]");
		
		
	}
	
}
