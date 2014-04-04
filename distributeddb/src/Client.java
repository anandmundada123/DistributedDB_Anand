package distributeddb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.zookeeper.ClientWatchManager;



@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

	private static final Log LOG = LogFactory.getLog(Client.class);

	// Configuration
	private Configuration conf;
	private YarnClient yarnClient;
	// Application master specific info to register a new Application with RM/ASM
	private String appName = "";
	// App master priority
	private int amPriority = 0;
	// Queue for App master
	private String amQueue = "";
	// Amt. of memory resource to request for to run the App Master
	private int amMemory = 10; 
	// Application master jar file
	private String appMasterJar = "distributedDB.jar"; 
	// Main class to invoke application master
	private final String appMasterMainClass = "distributeddb.ApplicationMaster";
	// Query to execute
	//private String query = ""; 
	// Node where to launch container
	// private String node = "";
	// Port to listen to
	private int clientListentPort = -1;
	// Amt of memory to request for container in which shell script will be executed
	private int containerMemory = 10; 
	// No. of containers in which the shell script needs to be executed
	private int numContainers = 1;
	// Start time for client
	private final long clientStartTime = System.currentTimeMillis();
	// Timeout threshold for client. Kill app after time interval expires.
	private long clientTimeout = 600000;
	// Debug flag
	boolean debugFlag = false;	
	// Command line options
	private Options opts;
	// TCPServer object to get queries and return results
	private TCPServer tcpServer;
	// Application master host name
	private String appMasterHostName;
	// Client Host Name
	private String clientHostName;
	// App Master Listen Port
	private int appMasterPortNumber;
	// Hashmap to keep track of ports for containers on each node in hadoop cluster
	private HashMap<String, Integer> containerMap;
	// Count to decide how many updates are required at client
	// init to 1 as for sure we need host and port update from Appmaster
	// increment this count for each container
	private int updateCnt = 1;
	/**
	 * @param args Command line arguments 
	 */
	public static void main(String[] args) {
		try {
			Client client = new Client();
			try {
				// get all command line arguments
				boolean doRun = client.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				client.printUsage();
				System.exit(-1);
			}

			client.run();
		} catch (Throwable t) {
			LOG.fatal("Error running CLient", t);
			System.exit(1);
		}
	}

	/**
	 */
	public Client(Configuration conf) throws Exception  {

		this.conf = conf;
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		opts = new Options();
		opts.addOption("appname", true, "Application Name. Default value - DistributedDB");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
		opts.addOption("timeout", true, "Application timeout in milliseconds");
		opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
		opts.addOption("jar", true, "Jar file containing the application master");
		//opts.addOption("query", true, "The distributed query to execute");
		//opts.addOption("port", true, "The port number to listen to");
		// Anand TODO
		//opts.addOption("node", true, "Node name where you want to launch container");
		opts.addOption("container_memory", true, "Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true, "No. of containers on which the shell command needs to be executed");
		opts.addOption("debug", false, "Dump out debug information");
		opts.addOption("help", false, "Print usage");
	}

	/**
	 */
	public Client() throws Exception  {
		this(new YarnConfiguration());
	}

	/**
	 * Helper function to print out usage
	 */
	private void printUsage() {
		new HelpFormatter().printHelp("Client", opts);
	}

	/**
	 * Parse command line options
	 * @param args Parsed command line options 
	 * @return Whether the init was successful to run the client
	 * @throws ParseException
	 */
	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);

		/*if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }*/

		if (cliParser.hasOption("help") || cliParser.hasOption("h")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("debug")) {
			debugFlag = true;

		}

		appName = cliParser.getOptionValue("appname", "DistributedDB");
		amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
		amQueue = cliParser.getOptionValue("queue", "default");
		amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));		

		if (amMemory < 0) {
			throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
					+ " Specified memory=" + amMemory);
		}

		//DFW: adding a default value so we don't have to pass this param
		if (cliParser.hasOption("jar")) {
			appMasterJar = cliParser.getOptionValue("jar", "distributedDB.jar");
		}		

		// New: Get Client Host Name. Don't need to hard code them otherwise 
		// it will not work on other machines
		try {
			clientHostName = DDBUtil.getHostName();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// New: We will find out free port number 
		try {
			clientListentPort = DDBUtil.findFreePort();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// Set up the server
		try {
			tcpServer = new TCPServer(clientHostName, clientListentPort, LOG);
			LOG.info("Starting TCP Server on port " + clientListentPort);
			tcpServer.run();
		} catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
			System.exit(-1);
		}


		//TODO: input node is used for all queries
		/*if (cliParser.hasOption("node")) {
			node = cliParser.getOptionValue("node", "master");
		} else {
			throw new IllegalArgumentException("Node required");
		}*/

		containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));
		numContainers = Integer.parseInt(cliParser.getOptionValue("num_containers", "1"));

		if (containerMemory < 0 || numContainers < 1) {
			throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
					+ " Specified containerMemory=" + containerMemory
					+ ", numContainer=" + numContainers);
		}

		clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

		return true;
	}

	/* private static String readFromConsole() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String s = br.readLine();
		return s;       
	}
	 */
	private static void writeResultToConsole(TCPServer tcp, FileSystem fs, ApplicationId appId) throws IOException {
		//First copy the output file from HDFS to local  
		String fileName = new String("/tmp/output_" + appId.getId());
		Path ansSrc = new Path(fs.getHomeDirectory(), "output_" + appId.getId());
		Path ansDst = new Path(fileName);
		LOG.info("fs.getHomeDirectory " + fs.getHomeDirectory().getName());
		LOG.info("ansSrc: " + ansSrc.getName());
		LOG.info("ansDst: " + ansDst.getName());
		try {
			fs.moveToLocalFile(ansSrc, ansDst);
		} catch (IOException e) {
			LOG.error("Unable to copy output file to local directory: " + e.getMessage());
		}

		//Now read the file
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		StringBuilder ans = new StringBuilder();
		try {
			String line = br.readLine();
			while(line != null){
				ans.append(line);
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		// Delete the file after reading from tmp and HDFS
		/*
		 * FIXME: uncomment following lines
		 */
		//fs.delete(ansSrc, false);
		//fs.delete(ansDst, false);

		// FIXME: right now writing to console 
		// Fix it to write to tcp connection
		
		LOG.info(ans.toString());
		// Send the answer to the tcpServer as the result
		//tcp.sendResult(ans.toString());
	}

	/**
	 * Main run function for the client
	 * @return true if application completed successfully
	 * @throws IOException
	 * @throws YarnException
	 */
	public void run() throws IOException, YarnException {

		containerMap = new HashMap<String, Integer>();
		
		yarnClient.start();
		String nodeList = "";
		YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
		LOG.info("Got Cluster metric info from ASM" 
				+ ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());
		
		List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
				NodeState.RUNNING);
		LOG.info("Got Cluster node info from ASM");
		for (NodeReport node : clusterNodeReports) {
			LOG.info("Got node report from ASM for"
					+ ", nodeId=" + node.getNodeId().getHost()
					+ ", nodeAddress" + node.getHttpAddress()
					+ ", nodeRackName" + node.getRackName()
					+ ", nodeNumContainers" + node.getNumContainers());
			
			// Add All nodes name in container map
			containerMap.put(node.getNodeId().getHost(), -1);
			updateCnt++;
			if(nodeList.equals("")) {
			nodeList = nodeList + node.getNodeId().getHost();
			} else {
				nodeList = nodeList + "," + node.getNodeId().getHost();
			}
		}

		QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
		LOG.info("Queue info"
				+ ", queueName=" + queueInfo.getQueueName()
				+ ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
				+ ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
				+ ", queueApplicationCount=" + queueInfo.getApplications().size()
				+ ", queueChildQueueCount=" + queueInfo.getChildQueues().size());		

		List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
		for (QueueUserACLInfo aclInfo : listAclInfo) {
			for (QueueACL userAcl : aclInfo.getUserAcls()) {
				LOG.info("User ACL Info for Queue"
						+ ", queueName=" + aclInfo.getQueueName()			
						+ ", userAcl=" + userAcl.name());
			}
		}		
		
		// Get a new application id
		YarnClientApplication app = yarnClient.createApplication();

		// set the application name
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		appContext.setApplicationName(appName);

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		// set local resources for the application master
		// local files or archives as needed
		// In this scenario, the jar file for the application master is part of the local resources			
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

		LOG.info("Copy App Master jar from local filesystem and add to local environment");
		// Copy the application master jar to the filesystem 
		// Create a local resource to point to the destination jar path 
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(appMasterJar);
		String pathSuffix = appName + "/" + appId.getId() + "/AppMaster.jar";	    
		Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus destStatus = fs.getFileStatus(dst);
		LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

		// Set the type of resource - file or archive
		// archives are untarred at destination
		// we don't need the jar file to be untarred for now
		amJarRsrc.setType(LocalResourceType.FILE);
		// Set visibility of the resource 
		// Setting to most private option
		amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);	   
		// Set the resource to be copied over
		amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst)); 
		// Set timestamp and length of file so that the framework 
		// can do basic sanity checks for the local resource 
		// after it has been copied over to ensure it is the same 
		// resource the client intended to use with the application
		amJarRsrc.setTimestamp(destStatus.getModificationTime());
		amJarRsrc.setSize(destStatus.getLen());
		localResources.put("AppMaster.jar",  amJarRsrc);

		// The shell script has to be made available on the final container(s)
		// where it will be executed. 
		// To do this, we need to first copy into the filesystem that is visible 
		// to the yarn framework. 
		// We do not need to set this as a local resource for the application 
		// master as the application master does not need it. 		
		String hdfsDbShellScriptLocation = ""; 
		String hdfsWrapShellScriptLocation = ""; 
		String hdfsContainerShellScriptLocation = ""; 
		
		long hdfsDbShellScriptLen = 0;
		long hdfsWrapShellScriptLen = 0;
		//long hdfsContainerShellScriptLen = 0;
		
		long hdfsDbShellScriptTimestamp = 0;
		long hdfsWrapShellScriptTimestamp = 0;
		//long hdfsContainerShellScriptTimestamp = 0;
		
		// Copy the required scripts so they are local resources to the worker nodes
		String dbScriptPath = DDBConstants.DB_SCRIPT_LOCATION;
		String wrapScriptPath = DDBConstants.WRAP_SCRIPT_LOCATION;
		//String containerScriptPath = DDBConstants.CONTAINER_SCRIPT_LOCATION;
		
		Path dbShellSrc = new Path(dbScriptPath);
		Path wrapShellSrc = new Path(wrapScriptPath);
		//Path containerShellSrc = new Path(containerScriptPath);
		
		String dbShellPathSuffix = appName + "/" + DDBConstants.DB_SCRIPT_LOCATION;
		String wrapShellPathSuffix = appName + "/" + DDBConstants.WRAP_SCRIPT_LOCATION;
		//String containerShellPathSuffix = appName + "/" + DDBConstants.CONTAINER_SCRIPT_LOCATION;
		
		Path dbShellDst = new Path(fs.getHomeDirectory(), dbShellPathSuffix);
		Path wrapShellDst = new Path(fs.getHomeDirectory(), wrapShellPathSuffix);
		//Path containerShellDst = new Path(fs.getHomeDirectory(), containerShellPathSuffix);
		
		fs.copyFromLocalFile(false, true, dbShellSrc, dbShellDst);
		fs.copyFromLocalFile(false, true, wrapShellSrc, wrapShellDst);
		//fs.copyFromLocalFile(false, true, containerShellSrc, containerShellDst);
		
		hdfsDbShellScriptLocation = dbShellDst.toUri().toString(); 
		hdfsWrapShellScriptLocation = wrapShellDst.toUri().toString(); 
		//hdfsContainerShellScriptLocation = containerShellDst.toUri().toString(); 
		
		FileStatus dbShellFileStatus = fs.getFileStatus(dbShellDst);
		FileStatus wrapShellFileStatus = fs.getFileStatus(wrapShellDst);
		//FileStatus containerShellFileStatus = fs.getFileStatus(containerShellDst);
		
		hdfsDbShellScriptLen = dbShellFileStatus.getLen();
		hdfsWrapShellScriptLen = wrapShellFileStatus.getLen();
		//hdfsContainerShellScriptLen = containerShellFileStatus.getLen();
		
		hdfsDbShellScriptTimestamp = dbShellFileStatus.getModificationTime();
		hdfsWrapShellScriptTimestamp = wrapShellFileStatus.getModificationTime();
		//hdfsContainerShellScriptTimestamp = containerShellFileStatus.getModificationTime();
		
		// Set local resource info into app master container launch context
		amContainer.setLocalResources(localResources);

		// Set the necessary security tokens as needed
		//amContainer.setContainerTokens(containerToken);

		// Set the env variables to be setup in the env where the application master will be run
		LOG.info("Set the environment for the application master");
		Map<String, String> env = new HashMap<String, String>();

		// put location of shell script into env
		// using the env info, the application master will create the correct local resource for the 
		// eventual containers that will be launched to execute the shell scripts
		env.put(DDBConstants.DDB_DB_LOCATION, hdfsDbShellScriptLocation);
		env.put(DDBConstants.DDB_DB_TIMESTAMP, Long.toString(hdfsDbShellScriptTimestamp));
		env.put(DDBConstants.DDB_DB_LEN, Long.toString(hdfsDbShellScriptLen));
		
		env.put(DDBConstants.DDB_WRAP_LOCATION, hdfsWrapShellScriptLocation);
		env.put(DDBConstants.DDB_WRAP_TIMESTAMP, Long.toString(hdfsWrapShellScriptTimestamp));
		env.put(DDBConstants.DDB_WRAP_LEN, Long.toString(hdfsWrapShellScriptLen));
		
		// Add AppMaster.jar location to classpath 		
		// At some point we should not be required to add 
		// the hadoop specific classpaths to the env. 
		// It should be provided out of the box. 
		// For now setting all required classpaths including
		// the classpath to "." for the application jar
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
		.append(File.pathSeparatorChar).append("./*");
		for (String c : conf.getStrings(
				YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(File.pathSeparatorChar);
			classPathEnv.append(c.trim());
		}
		classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

		// add the runtime classpath needed for tests to work
		if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
			classPathEnv.append(':');
			classPathEnv.append(System.getProperty("java.class.path"));
		}

		env.put("CLASSPATH", classPathEnv.toString());

		amContainer.setEnvironment(env);

		// Set the necessary command to execute the application master 
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);

		// Set java executable command 
		LOG.info("Setting up app master command");
		vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
		// Set Xmx based on am memory size
		vargs.add("-Xmx" + amMemory + "m");
		// Set class name 
		vargs.add(appMasterMainClass);
		// Set params for Application Master
		vargs.add("--container_memory " + String.valueOf(containerMemory));
		vargs.add("--num_containers " + String.valueOf(numContainers));
		//NOTE: The query is a sentence and so we must surround it by quotes otherwise it won't get parsed properly by the ApplicationMaster
		//vargs.add("--query '" + query + "'");
		//Anand
		//vargs.add("--node " + node);
		
		//NEW: Add Client Hostname and Client Listening port
		vargs.add("--" + DDBConstants.CLIENT_HOST_NAME + " " + clientHostName);
		vargs.add("--" + DDBConstants.CLIENT_PORT_NO + " " + clientListentPort);
		
		// Add all nodes List 
		vargs.add("--nodes " + nodeList);
		if (debugFlag) {
			vargs.add("--debug");
		}

		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

		// Get final commmand
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}

		LOG.info("Completed setting up app master command " + command.toString());	   
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());		
		amContainer.setCommands(commands);

		// Set up resource type requirements
		// For now, only memory is supported so we set memory requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(amMemory);
		appContext.setResource(capability);

		// Service data is a binary blob that can be passed to the application
		// Not needed in this scenario
		// amContainer.setServiceData(serviceData);

		// The following are not required for launching an application master 
		// amContainer.setContainerId(containerId);		

		appContext.setAMContainerSpec(amContainer);
		// Set the priority for the application master
		Priority pri = Records.newRecord(Priority.class);
		// TODO - what is the range for priority? how to decide? 
		pri.setPriority(amPriority);
		appContext.setPriority(pri);

		// Set the queue to which this application is to be submitted in the RM
		appContext.setQueue(amQueue);

		// Submit the application to the applications manager
		// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
		// Ignore the response as either a valid response object is returned on success 
		// or an exception thrown to denote some form of a failure
		LOG.info("Submitting application to ASM");

		yarnClient.submitApplication(appContext);

		
		/**
		 * Waiting for all updates 
		 */
		
		for(int i = 0; i < updateCnt; i++) {
			String msg = getNextMsg(tcpServer);
			LOG.info("\n Anand: Got Update Msg: " + msg);
			if(msg.startsWith(DDBConstants.APP_MASTER_INFO)) {
				String msgArray[] = msg.split(" ");
				if(msgArray.length == 3) {
					LOG.info("\nAnand: App master host name : "+ msgArray[1]);
					LOG.info("\nAnand: App master port number : "+ msgArray[2]);
					appMasterHostName = msgArray[1];
					appMasterPortNumber = Integer.parseInt(msgArray[2]);
				} else {
					LOG.info("\nAnand: Got message with less than 3 arguments from AppMaster");
					tcpServer.close();
					System.exit(0);
				}
			} else if(msg.startsWith("connect")) {
				String msgArray[] = msg.split(" ");
				if(msgArray.length == 3) {
					LOG.info("\nAnand: Container host name : "+ msgArray[1]);
					LOG.info("\nAnand: Container port number : "+ msgArray[2]);
					containerMap.put(msgArray[1], Integer.parseInt(msgArray[2]));
				} else {
					LOG.info("\nAnand: Got message with less than 3 arguments from container");
					tcpServer.close();
					System.exit(0);
				}
			}
		}
		
		/**
		 * NEW: Open a client connection with AppMaster 
		 */
		
		
		/*
		 * NEW: wait for queries and then send that query to Application master
		 * and wait to get result from application master
		 */
		TCPClient client = null;
		while(true) {
			LOG.info("Enter Query: ");
			String query = getNextMsg(tcpServer);
			LOG.info("Got Query:"+query+":");
			if(query.startsWith("exit")) {
				LOG.info("Exiting as got exit from user");
				if(client != null)
					client.closeConnection();
				client = new TCPClient(appMasterHostName, appMasterPortNumber);
				client.init();
				client.sendMsg("exit");
				//client.closeConnection();
				tcpServer.close();
				forceKillApplication(appId);
				System.exit(0);
			}
			int ind = query.indexOf(".");
			if(ind == -1) {
				LOG.info("Query should start with node name");
				LOG.info("<node>.<query>");
				continue;
			}
			String node = query.substring(0, ind);
			query = query.substring(ind+1);
			
			// Send query to Application manager
			if (client != null)
				client.closeConnection();
			client = new TCPClient(node, containerMap.get(node));
			client.init();
			client.sendMsg(query);
			// wait for reply from Application manager
			String msg = getNextMsg(tcpServer);
			LOG.info("\n Anand: Got Msg from Container: " + msg);
			writeResultToConsole(tcpServer, fs, appId);
		}
		
	}
	
	
	private String getNextMsg(TCPServer tcpServer) {
		String query1 = null;
		while (true) {
			query1 = tcpServer.getNextQuery();
			if(query1 == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.warn("Thread interrupted from sleep?" + e.getLocalizedMessage());
				}
				continue;
			} else {
				query1 = query1.trim();
				break;
			}
		}
		return query1;
	}
	

	/**
	 * Monitor the submitted application for completion. 
	 * Kill application if time expires. 
	 * @param appId Application Id of application to be monitored
	 * @return true if application completed successfully
	 * @throws YarnException
	 * @throws IOException
	 */
	private boolean monitorApplication(ApplicationId appId)
			throws YarnException, IOException {

		while (true) {

			// Check app status every 1 second.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}

			// Get application report for the appId we are interested in 
			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got application report from ASM for"
					+ ", appId=" + appId.getId()
					+ ", clientToAMToken=" + report.getClientToAMToken()
					+ ", appDiagnostics=" + report.getDiagnostics()
					+ ", appMasterHost=" + report.getHost()
					+ ", appQueue=" + report.getQueue()
					+ ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime()
					+ ", yarnAppState=" + report.getYarnApplicationState().toString()
					+ ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
					+ ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;        
				}
				else {
					LOG.info("Application did finished unsuccessfully."
							+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
							+ ". Breaking monitoring loop");
					return false;
				}			  
			}
			else if (YarnApplicationState.KILLED == state	
					|| YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish."
						+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
						+ ". Breaking monitoring loop");
				return false;
			}			

			if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
				LOG.info("Reached client specified timeout for application. Killing application");
				forceKillApplication(appId);
				return false;				
			}
		}			

	}

	/**
	 * Kill a submitted application by sending a call to the ASM
	 * @param appId Application Id to be killed. 
	 * @throws YarnException
	 * @throws IOException
	 */
	private void forceKillApplication(ApplicationId appId)
			throws YarnException, IOException {
		// TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
		// the same time. 
		// If yes, can we kill a particular attempt only?

		// Response can be ignored as it is non-null on success or 
		// throws an exception in case of failures
		yarnClient.killApplication(appId);	
	}
}