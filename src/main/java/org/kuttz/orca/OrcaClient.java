package org.kuttz.orca;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.kuttz.orca.controller.OrcaControllerArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaClient extends YarnClientImpl{
	
	private static Logger logger = LoggerFactory.getLogger(OrcaClient.class);

	private final Configuration conf;
	private final OrcaControllerArgs orcaArgs;
	
	public static void main(String[] args) {
		YarnConfiguration yarnConf = new YarnConfiguration();
		
		OrcaControllerArgs orcaArgs = new OrcaControllerArgs();
		logger.info("OrcaClient args = " + Arrays.toString(args));
		Tools.parseArgs(orcaArgs, args);
		
		OrcaClient client = new OrcaClient(orcaArgs, yarnConf);
		boolean result = false;
		try {
			result = client.run();
		} catch (Exception e) {
			logger.error("Caught exception !!", e);
			System.exit(1);
		}
		if (result) {
			logger.info("Orca Application completed successfully..");
			System.exit(0);
		}
		logger.info("Orca Application failed..");
		System.exit(1);		
	}
	
	public OrcaClient(OrcaControllerArgs orcaArgs, Configuration conf) {
		this.conf = conf;
		this.orcaArgs = orcaArgs;
		init(this.conf);
		start();
	}
	
	public boolean run() throws IOException, YarnException {
		logger.info("Running Orca client..");
		start();
		
		YarnClusterMetrics clusterMetrics = super.getYarnClusterMetrics();
		logger.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());
		
		List<NodeReport> clusterNodeReports = super.getNodeReports();
		logger.info("Got Cluster node info from ASM");
		for (NodeReport node : clusterNodeReports) {
		    logger.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress="
		            + node.getHttpAddress() + ", nodeRackName=" + node.getRackName() + ", nodeNumContainers="
		            + node.getNumContainers() + ", nodeHealthStatus=" + node.getHealthReport());
		}

		QueueInfo queueInfo = super.getQueueInfo("default");
		logger.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
		        + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
		        + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount="
		        + queueInfo.getChildQueues().size());

		List<QueueUserACLInfo> listAclInfo = super.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
		    for (QueueACL userAcl : aclInfo.getUserAcls()) {
		        logger.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
		                + userAcl.name());
		    }
		}

		// Get a new application id
		YarnClientApplication app = super.createApplication();
		GetNewApplicationResponse newApp = app.getNewApplicationResponse();
		ApplicationId appId = newApp.getApplicationId();

		// TODO get min/max resource capabilities from RM and change memory ask if needed
		// If we do not have min/max, we may not be able to correctly request
		// the required resources from the RM for the app master
		// Memory ask has to be a multiple of min and less than max.
		// Dump out information about cluster capability as seen by the resource manager
		int maxMem = newApp.getMaximumResourceCapability().getMemory();
		logger.info("Max mem capability of resources in this cluster " + maxMem);		
		
		logger.info("Setting up application submission context for ASM");
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
//		ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

		appContext.setKeepContainersAcrossApplicationAttempts(true);
		// set the application id
		appContext.setApplicationId(appId);
		// set the application name
		appContext.setApplicationName("OrcaApp");

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		
		// set local resources for the application master
		// local files or archives as needed
		 // In this scenario, the jar file for the application master is part of the local resources		
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		
		// TODO avoid depending on source distribution paths
		File[] classPathFiles = new File(orcaArgs.orcaDir).listFiles();
		FileSystem fs = FileSystem.get(conf);

		for (int i = 0; i < classPathFiles.length; i++) {
		    Path dest = copyToLocalResources(appId, fs, localResources, classPathFiles[i]);
		    logger.info("Copied classpath resource " + classPathFiles[i].getAbsolutePath() + " to "
		            + dest.toUri().toString());
		}
		
		// Set local resource info into app master container launch context
		amContainer.setLocalResources(localResources);		
		
        // Set the env variables to be setup in the env where the application master will be run
		logger.info("Set the environment for the application master");
 		Map<String, String> env = new HashMap<String, String>();		
		
 		// For now setting all required classpaths including
 		// the classpath to "." for the application jar
 		StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");
 		for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
 				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
 	    	classPathEnv.append(':');
 	    	classPathEnv.append(c.trim());
 		}
 		classPathEnv.append(":./log4j.properties");

 		env.put("CLASSPATH", classPathEnv.toString()); 		
 		
 		amContainer.setEnvironment(env);
 		
 		// Set the necessary command to execute the application master
 		Vector<CharSequence> vargs = new Vector<CharSequence>(30);

 		// Set java executable command
 		logger.info("Setting up app master command");
 		logger.info("Classpath : [" + classPathEnv.toString() + "]");

 		// vargs.add("${JAVA_HOME}" + "/bin/java");
 		// TODO set java from JAVA_HOME
 		
 		logger.info("Logs found in dir : [" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "]");
 		vargs.add("java");
 		if (orcaArgs.debug) {
 			vargs.add("-Xdebug");
 			vargs.add("-Xrunjdwp:transport=dt_socket,address=8888,server=y,suspend=n");
 		}
 		
 		vargs.add(OrcaAppMaster.class.getName()); 		
 		vargs.add("-num_containers");
 		vargs.add("" + orcaArgs.numContainers);
 		vargs.add("-hb_period");
 		vargs.add("" + orcaArgs.hbPeriod);
 		vargs.add("-hb_warn_time");
 		vargs.add("" + orcaArgs.hbWarnTime);
 		vargs.add("-hb_dead_time");
 		vargs.add("" + orcaArgs.hbDeadTime);
 		vargs.add("-hb_num_check_threads");
 		vargs.add("" + orcaArgs.hbCheckerThreads);
 		vargs.add("-hb_num_master_threads");
 		vargs.add("" + orcaArgs.hbMasterThreads);
 		vargs.add("-hb_min_port");
 		vargs.add("" + orcaArgs.hbMinPort);
 		vargs.add("-hb_max_port");
 		vargs.add("" + orcaArgs.hbMaxPort);
 		vargs.add("-elb_min_port");
 		vargs.add("" + orcaArgs.elbMinPort);
 		vargs.add("-elb_max_port");
 		vargs.add("" + orcaArgs.elbMaxPort);
 		vargs.add("-app_name");
 		vargs.add(orcaArgs.appName);
 		vargs.add("-war_location");
 		vargs.add(orcaArgs.warLocation); 		
 		vargs.add("1>/tmp/OrcaApplicationMaster.stdout");
 		vargs.add("2>/tmp/OrcaApplicationMaster.stderr");
 		
 		// Get final commmand
 		StringBuilder command = new StringBuilder();
 		for (CharSequence str : vargs) {
 		    command.append(str).append(" ");
 		}

 		logger.info("Completed setting up app master command " + command.toString());
 		List<String> commands = new ArrayList<String>();
 		commands.add(command.toString());
 		amContainer.setCommands(commands);
 		
		// Set up resource type requirements
		// For now, only memory is supported so we set memory requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(orcaArgs.containerMemory);
		appContext.setResource(capability);

		appContext.setAMContainerSpec(amContainer);
		
		// Set the priority for the application master
		Priority pri = Records.newRecord(Priority.class);
		// TODO - what is the range for priority? how to decide?
		pri.setPriority(orcaArgs.priority);
		appContext.setPriority(pri);

		// Set the queue to which this application is to be submitted in the RM
		appContext.setQueue("default");

		// TODO : DO some Orca specific stuff 

		logger.info("Submitting application to ASM");
		super.submitApplication(appContext);

		// TODO
		// Try submitting the same request again
		// app submission failure?

		// Monitor the application (TODO: optional?)

		return monitorApplication(appId);
	}
	
	private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
		
		int numRunning = 5;
		
		while(true) {
			try {
			    Thread.sleep(10000);
			} catch (InterruptedException e) {
			    logger.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in
			ApplicationReport report = super.getApplicationReport(appId);
			logger.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToken="
			         + report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
			         + report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
			         + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState="
			         + report.getYarnApplicationState().toString() + ", distributedFinalState="
			         + report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
			         + ", appUser=" + report.getUser());
			 
			 YarnApplicationState state = report.getYarnApplicationState();
			 FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			 if (YarnApplicationState.RUNNING == state) {
				 numRunning--;
				 if (numRunning < 1) {
					 logger.info("Looks like Application is Running fine.. Client signing off !!");
					 return true;
				 }
			 }
			 
			 if (YarnApplicationState.FINISHED == state) {
			     if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
			         logger.info("Application has completed successfully. Breaking monitoring loop");
			         return true;
			     } else {
			         logger.info("Application finished unsuccessfully !!." + " YarnState=" + state.toString()
			                 + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
			         return false;
			     }
			 } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
			     logger.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus="
			             + dsStatus.toString() + ". Breaking monitoring loop");
			     return false;
			 }
			 
			 
			 
//			 if ((yarnArgs.timeout != -1) && (System.currentTimeMillis() > (clientStartTime + yarnArgs.timeout))) {
//			     logger.info("Reached client specified timeout for application. Killing application");
//			     forceKillApplication(appId);
//			     return false;
//			 }			
		}
	}
	
	private Path copyToLocalResources(ApplicationId appId, FileSystem fs, Map<String, LocalResource> localResources,
	        File file) throws IOException {
	    Path src = new Path(file.getAbsolutePath());
	    logger.info("Copying resources SRC dir : " + src);

	    // TODO use home directory + appId / appName?
	    Path dst = new Path(new Path(fs.getHomeDirectory(), "/app-" + appId.getId()), file.getName());
	    logger.info("Copying resources DEST dir : " + dst);
	    logger.info("Filesystem : " + fs.getScheme());
	    fs.copyFromLocalFile(false, true, src, dst);
	    FileStatus destStatus = fs.getFileStatus(dst);
	    LocalResource resource = Records.newRecord(LocalResource.class);
	    resource.setType(LocalResourceType.FILE);
	    resource.setVisibility(LocalResourceVisibility.APPLICATION);
	    resource.setResource(ConverterUtils.getYarnUrlFromPath(dst));
	    // Set timestamp and length of file so that the framework
	    // can do basic sanity checks for the local resource
	    // after it has been copied over to ensure it is the same
	    // resource the client intended to use with the application
	    resource.setTimestamp(destStatus.getModificationTime());
	    resource.setSize(destStatus.getLen());
	    localResources.put(file.getName(), resource);
	    return dst;
	}
	

}
