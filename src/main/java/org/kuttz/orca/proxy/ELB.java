package org.kuttz.orca.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ELB implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(ELB.class);
			
	private int runningPort;
	
	private volatile boolean hasStarted;
	
	private TreeSet<ELBNode> nodeSet = new TreeSet<ELB.ELBNode>();
	
	private ELBNode currentNode = null;
	
	private ELBArgs elbArgs = null;
	
	private final Proxy container;
	
    public Map<ELBNode, Stats> nodeStats = new HashMap<ELBNode, Stats>();
    
    static class Stats {
      public int outstanding;
      public int total;
    }

    public void updateNodeStats() {
      container.updateNodeStats();
    }

	public ELB(ELBArgs args, Proxy container) {
		this.elbArgs = args;
		this.container = container;
	}
	
	public ELB addNode(String host, int port) {
		nodeSet.add(new ELBNode(host, port));
		return this;
	}	
	
	public ELB removeNode(String host, int port) {
		nodeSet.remove(new ELBNode(host, port));
		return this;
	}
	
	public synchronized ELBNode nextNode() {		
		if (currentNode == null) {
			currentNode = nodeSet.first();
		} else {
			currentNode = nodeSet.higher(currentNode);
			if (currentNode == null) {
				currentNode = nodeSet.first();
			}
		}
		return currentNode;
	}
	
	public void init() {
		// TODO: any initialization ?
	}	
	
	@Override
	public void run() {
		Executor cachedThreadPool = Executors.newCachedThreadPool();
		try {
			ServerBootstrap sb = 
					new ServerBootstrap(
							new NioServerSocketChannelFactory(cachedThreadPool, cachedThreadPool));
			NioClientSocketChannelFactory cf = 
					new NioClientSocketChannelFactory(cachedThreadPool, cachedThreadPool);			
			sb.setPipelineFactory(new ELBPipelineFactory(cf, this));			
			bindToPort(sb, elbArgs.minPort, elbArgs.maxPort);					
		} catch (Exception e) {
			logger.error("Could not start Heartbeat Master", e);
		}
	}
	
	private void bindToPort(ServerBootstrap sb, int minPort, int maxPort) throws IOException {
		for (int p = minPort; p <= maxPort; p++) {
			try {
				sb.bind(new InetSocketAddress(p));
				logger.info("Starting ELB Proxy on port [" + p + "]");
				this.hasStarted = true;
				this.runningPort = p;
				container.setELBPort(runningPort);
				return;				
			} catch (Exception e) {
				logger.info("Could not start ELB Proxy on port [" + p + "] !!");
				continue;
			}
		}
		
		throw new IOException("No free ports available from [" + minPort + " to " + maxPort + "]");
			
	}
	
	public boolean isRunning() {
		return this.hasStarted;
	}
	
	public int getRunningPort() {
		return this.runningPort;
	} 
	
	public static class ELBNode implements Comparable<ELBNode> {
		public final String host;
		public final int port;
		public ELBNode(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + port;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ELBNode other = (ELBNode) obj;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (port != other.port)
				return false;
			return true;
		}
		@Override
		public String toString() {
			return "ELBNode [host=" + host + ", port=" + port + "]";
		}
		@Override
		public int compareTo(ELBNode o) {
			return ("" + host + port).compareTo("" + o.host + o.port);
		}		
	}
	
	
	public static void main(String[] args) {
//		OrcaELB elb = new OrcaELB(8811).addNode("localhost", 8812).addNode("localhost", 8813);
//		elb.run();		
	}

}
