package org.kuttz.orca.controller;

import com.beust.jcommander.Parameter;

public class OrcaControllerArgs {

	@Parameter(names = "-container_mem", description = "YARN parameter: Container memory", required = false)
	public int containerMemory = 256;
	
	@Parameter(names = "-priority", description = "Priority ??", required = false)
	public int priority = 0;
	
	@Parameter(names = "-user", description = "User name", required = false)
	public String user = "";	
	
	@Parameter(names = "-num_containers", description = "YARN parameter: Number of containers on which the Orca needs to be hosted", required = true)
	public int numContainers = 3;

	@Parameter(names = "-hb_period", description = "Heartbeat period", required = false)
	public int hbPeriod = 1000;
	
	@Parameter(names = "-hb_warn_time", description = "Time since last heartbeat for issuing a warning to client", required = false)
	public int hbWarnTime = 15000;
	
	@Parameter(names = "-hb_dead_time", description = "Time since last heartbeat for issuing a node dead msg to client", required = false)
	public int hbDeadTime = 30000;	
	
	@Parameter(names = "-hb_num_check_threads", description = "Number of threads allocated to checking the status of all nodes", required = false)
	public int hbCheckerThreads = 1;
	
	@Parameter(names = "-hb_num_master_threads", description = "Number of threads for the Heartbeat master", required = false)
	public int hbMasterThreads = 2;
	
	@Parameter(names = "-hb_min_port", description = "Min port on which HB master would listen on", required = false)
	public int hbMinPort = 8100;
	
	@Parameter(names = "-hb_max_port", description = "Miax port on which HB master would listen on", required = false)
	public int hbMaxPort = 8200;
	
	@Parameter(names = "-elb_min_port", description = "Min port on which ELB would listen on", required = false)
	public int elbMinPort = 8100;
	
	@Parameter(names = "-elb_max_port", description = "Max port on which ELB would listen on", required = false)
	public int elbMaxPort = 8200;
	
	@Parameter(names = "-app_name", description = "Application Name", required = true)
	public String appName;
	
	@Parameter(names = "-war_location", description = "Location of .war file", required = true)
	public String warLocation;
	
	@Parameter(names = "-orca_dir", description = "Orca installation directory", required = true)
	public String orcaDir;
	
	@Parameter(names = "-jvm_debug", description = "Orca installation directory", required = false)
	public boolean debug = false;	
}
