package org.kuttz.orca.proxy;

import com.beust.jcommander.Parameter;

public class ELBArgs {
	
	@Parameter(names = "-min_port", description = "Min Port to bind to", required = true)	
	public int minPort;
	
	@Parameter(names = "-max_port", description = "Max Port to bind to", required = true)	
	public int maxPort;

}
