// =============================================================================
// This file is part of The PADRES Project.
//
// For more information, see http://www.msrg.utoronto.ca
//
// Copyright (c) 2003 Middleware Systems Research Group, University of Toronto
// =============================================================================
// $Id$
// =============================================================================

/*
 * Created on 16-Jul-2003
 */
package ca.utoronto.msrg.padres.broker.brokercore;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.Timer;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig.CycleType;
import ca.utoronto.msrg.padres.broker.controller.Controller;
import ca.utoronto.msrg.padres.broker.controller.LinkInfo;
import ca.utoronto.msrg.padres.broker.controller.OverlayManager;
import ca.utoronto.msrg.padres.broker.controller.OverlayRoutingTable;
import ca.utoronto.msrg.padres.broker.management.console.ConsoleInterface;
import ca.utoronto.msrg.padres.broker.management.web.ManagementServer;
import ca.utoronto.msrg.padres.broker.monitor.SystemMonitor;
import ca.utoronto.msrg.padres.broker.router.Router;
import ca.utoronto.msrg.padres.broker.router.RouterFactory;
import ca.utoronto.msrg.padres.broker.router.matching.MatcherException;
import ca.utoronto.msrg.padres.broker.webmonitor.monitor.WebUIMonitor;
import ca.utoronto.msrg.padres.client.BrokerState;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.comm.CommSystem;
import ca.utoronto.msrg.padres.common.comm.CommunicationException;
import ca.utoronto.msrg.padres.common.comm.MessageListenerInterface;
import ca.utoronto.msrg.padres.common.comm.MessageQueue;
import ca.utoronto.msrg.padres.common.comm.MessageSender;
import ca.utoronto.msrg.padres.common.comm.NodeAddress;
import ca.utoronto.msrg.padres.common.comm.OutputQueue;
import ca.utoronto.msrg.padres.common.comm.QueueHandler;
import ca.utoronto.msrg.padres.common.comm.CommSystem.HostType;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.MessageDestination.DestinationType;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.common.util.LogException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.common.util.timer.TimerThread;

/**
 * The core of the broker. The broker is instantiated through this class. BrokerCore provides unique
 * message ID generation, component location, message routing.
 * 
 * @author eli
 * 
 */
public class BrokerCore {

	protected BrokerConfig brokerConfig;

	protected Controller controller;

	protected SystemMonitor systemMonitor;

	protected QueueManager queueManager;

	protected InputQueueHandler inputQueue;

	protected Router router;

	protected WebUIMonitor webuiMonitor;

	protected TimerThread timerThread;

	protected HeartbeatPublisher heartbeatPublisher;

	protected HeartbeatSubscriber heartbeatSubscriber;

	protected MessageDestination brokerDestination;

	protected int currentMessageID;

	protected CommSystem commSystem;

	protected boolean debug = BrokerConfig.DEBUG_MODE_DEFAULT;

	// Indicates whether this broker is running or not
	protected boolean running = false;

	protected boolean isCycle = false;

	protected boolean isDynamicCycle = false;

	// for dynamic cycle, check message rate. the time_window_interval can also be defined in the
	// broker property file
	protected int time_window_interval = 5000;

	private boolean isShutdown = false;

	protected static Logger brokerCoreLogger;

	protected static Logger exceptionLogger;
	
	private String uriForOverLoadedBroker = "";
	
	private boolean isLoadAcceptingBroker = false; 
	//CssInfo[] infoVector = new CssInfo[100]; // change Array size to subscriptionArray.size()
    List<CssInfo> infoVector = new ArrayList<CssInfo>();
    
    protected Map<NodeAddress, BrokerState> brokerStates = new HashMap<NodeAddress, BrokerState>();
	/**
	 * Constructor for one argument. To take advantage of command line arguments, use the
	 * 'BrokerCore(String[] args)' constructor
	 * 
	 * @param arg
	 * @throws IOException
	 */
	public BrokerCore(String arg) throws BrokerCoreException {
		this(arg.split("\\s+"));
	}

	public BrokerCore(String[] args, boolean def) throws BrokerCoreException {
		if (args == null) {
			throw new BrokerCoreException("Null arguments");
		}
		
		if (args.length > 0 && args[args.length-1].equals("loadbalance"))
		{
			isLoadAcceptingBroker = true;
		}
		CommandLine cmdLine = new CommandLine(BrokerConfig.getCommandLineKeys());
		try {
			cmdLine.processCommandLine(args);
		} catch (Exception e) {
			throw new BrokerCoreException("Error processing command line", e);
		}
		// make sure the logger is initialized before everything else
		initLog(cmdLine.getOptionValue(BrokerConfig.CMD_ARG_FLAG_LOG_LOCATION));
		brokerCoreLogger.debug("BrokerCore is starting.");
		// load properties from given/default properties file get the broker configuration
		String configFile = cmdLine.getOptionValue(BrokerConfig.CMD_ARG_FLAG_CONFIG_PROPS);
		try {
			if (configFile == null)
				brokerConfig = new BrokerConfig();
			else
				brokerConfig = new BrokerConfig(configFile, def);
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal(e.getMessage(), e);
			exceptionLogger.fatal(e.getMessage(), e);
			throw e;
		}
		// overwrite the configurations from the config file with the configurations from the
		// command line
		brokerConfig.overwriteWithCmdLineArgs(cmdLine);
		// check broker configuration
		try {
			brokerConfig.checkConfig();
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal("Missing uri key or uri value in the property file.");
			exceptionLogger.fatal("Here is an exception : ", e);
			throw e;
		}
		// initialize the message sequence counter
		currentMessageID = 0;
	}
	
	/**
	 * Constructor
	 * 
	 * @param args
	 */
	public BrokerCore(String[] args) throws BrokerCoreException {
		this(args, true);
	}

	public void cssBitVectorCalculation(){
		buildCSSVector();
		List<CssInfo> finalList = new ArrayList<CssInfo>();
		
		// Do something with this sleep.... NOT GOOD
		try {
			System.out.println("BrokerCore going to sleep................*******************");
			Thread.sleep(10000);
			System.out.println("BrokerCore waking up................*******************");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(this.infoVector == null)
		{
			System.out.println("BrokerCore >> cssBitVectorCalculation >> infovector NULL");
		}
		Collections.sort(infoVector,new CssInfoComparator());
		int sum = 0;
		int partialSum = 0;
		System.out.println("infoVector size >>>>>>>>>>> " + infoVector.size());
		for(CssInfo info : infoVector)
		{
			sum += info.getMatchingSubscriptions();
		}
		for(CssInfo info : infoVector)
		{
			if(partialSum <= sum/2)
			{
				partialSum += info.getMatchingSubscriptions();
				finalList.add(info);
			}
		}
		
		System.out.println("Final subscription list to be offloaded : \n");
		for(CssInfo info : finalList)
		{
			System.out.println(info.getCssClass() + "\n");
		}
	}
	
	public List<CssInfo> buildCSSVector(){
		System.out.println("BrokerCore >> buildCSSVector");
        
		Map<String, SubscriptionMessage> subs =  this.getSubscriptions();
		System.out.println("BrookerCore >> buildCSSVector >> subscriptions retrieved :" + subs);
		//String[] subscriptionArray = new String[subs.size()];
		List<String> subscriptionArray = new ArrayList<String>();
		//CssInfo[] infoVector = new CssInfo[subscriptionArray.size()];
		
		Iterator<Map.Entry<String, SubscriptionMessage>> it = subs.entrySet().iterator();
		while(it.hasNext())
		{
	
			Entry<String, SubscriptionMessage> thisEntry = it.next();
			String brokerInfoMsg = thisEntry.getValue().getSubscription().getClassVal();
			if(brokerInfoMsg.equalsIgnoreCase("HEARTBEAT_MANAGER")||brokerInfoMsg.equalsIgnoreCase("NETWORK_DISCOVERY")||brokerInfoMsg.equalsIgnoreCase("BROKER_INFO")||brokerInfoMsg.equalsIgnoreCase("GLOBAL_FD")||brokerInfoMsg.equalsIgnoreCase("BROKER_CONTROL")||brokerInfoMsg.equalsIgnoreCase("CSStobeMigrated")||brokerInfoMsg.equalsIgnoreCase("BROKER_MONITOR") )
			{
			  System.out.println("brokerCore >> BrokerINFO message skipped and not added to infovector");
			}
			else
			{
				subscriptionArray.add(thisEntry.getValue().getSubscription().getClassVal());
			}
		}
		System.out.println("brokerCore >> buildCSSVctor >> subscriptionArray : " + subscriptionArray);
		//String[] subscriptionArray = {"sports","stocks","movies"};
		for(int i=0; i<subscriptionArray.size(); i++)
		{
			System.out.println("BrokerCore >> buildCSSVector >> adding to infovector : " + subscriptionArray.get(i));
			CssInfo info = new CssInfo(subscriptionArray.get(i));
			infoVector.add(info);
		}
		
		System.out.println("BrokerCore >> buildCSSVector >> infovector : "+this.infoVector); //+">>"+this.infoVector.get(0).getClass());
		
		PublicationSensor pubSensor = new PublicationSensor(this);
		System.out.println("BrokerCore >> buildCSSVector >> PublicationSensor created");
		pubSensor.start();
		System.out.println("BrokerCore >> buildCSSVector >> PublicationSensor started");
		//checkPublications(infoVector);
		return infoVector;
	}
	
	public void checkPublications(CssInfo[] infoVector)
	{
		infoVector[0].setMatchingSubscriptions(10);
		infoVector[1].setMatchingSubscriptions(4);
		infoVector[2].setMatchingSubscriptions(7);
	}
	
	public void notifyBroker(PublicationMessage msg)
	{
		System.out.println("Publication Message Received : " + msg.getPublication());
		System.out.println("Publication Message Received : " + msg.getPublication().getClassVal());
		for(int i=0; i<infoVector.size(); i++)
		{
			if(infoVector.get(i).getCssClass().contains(msg.getPublication().getClassVal()))
			{
				infoVector.get(i).setMatchingSubscriptions(infoVector.get(i).getMatchingSubscriptions()+1);
			}
		}
	}
	
	public BrokerCore(BrokerConfig brokerConfig) throws BrokerCoreException {
		// make sure the logger is initialized before everything else
		
		initLog(brokerConfig.getLogDir());
		brokerCoreLogger.debug("BrokerCore is starting.");
		this.brokerConfig = brokerConfig;
		try {
			this.brokerConfig.checkConfig();
		} catch (BrokerCoreException e) {
			brokerCoreLogger.fatal("Missing uri key or uri value in the property file.");
			exceptionLogger.fatal("Here is an exception : ", e);
			throw e;
		}
		currentMessageID = 0;
	}

	protected void initLog(String logPath) throws BrokerCoreException {
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			try {
				new LogSetup(logPath);
			} catch (LogException e) {
				throw new BrokerCoreException("Initialization of Logger failed: ", e);
			}
		}
		brokerCoreLogger = Logger.getLogger(BrokerCore.class);
		exceptionLogger = Logger.getLogger("Exception");
	}

	/**
	 * Initialize the broker. It has to be called externally; the constructor does not use this
	 * method. Components are started up in a particular order, and initialize() doesn't return
	 * until the broker is fully started.
	 * 
	 * @throws BrokerCoreException
	 */
	public void initialize() throws BrokerCoreException {
		// Initialize some parameters
		isCycle = brokerConfig.isCycle();
		isDynamicCycle = brokerConfig.getCycleOption() == CycleType.DYNAMIC;
		// Initialize components
		initCommSystem();
		brokerDestination = new MessageDestination(getBrokerURI(), DestinationType.BROKER);
		System.out.println("BrokerCore >> initialize >> brokerDestination.getDestinationID : " + brokerDestination.getDestinationID());
		System.out.println("BrokerCore >> initialize >> brokerDestination.getBrokerID : " + brokerDestination.getBrokerId());
		initQueueManager();
		System.out.println("BrokerCore >> initialize >> initQueueManager() done");
		initRouter();
		System.out.println("BrokerCore >> initialize >> initRouter() done");
		initInputQueue();
		System.out.println("BrokerCore >> initialize >> initInputQueue() done");
		// System monitor must be started before sending/receiving any messages
		initSystemMonitor();
		System.out.println("BrokerCore >> initialize >> initSystemMonitor done");
		initController();
		System.out.println("BrokerCore >> initialize >> initController() done");
		startMessageRateTimer();
		System.out.println("BrokerCore >> initialize >> startMessageRateTimer() done");
		initTimerThread();
		System.out.println("BrokerCore >> initialize >> initTimerThread() done");
		initHeartBeatPublisher();
		System.out.println("BrokerCore >> initialize >> initHeartBeatPublisher() done");
		initHeartBeatSubscriber();
		System.out.println("BrokerCore >> initialize >> initHeartBeatSubscriber() done");
		initWebInterface();
		System.out.println("BrokerCore >> initialize >> initWebInterface() done");
		initNeighborConnections();
		System.out.println("BrokerCore >> initialize >> initNeighbourConnections() done");
		initManagementInterface();
		System.out.println("BrokerCore >> initialize >> initManagementInterface() done");
		initConsoleInterface();
		uriForOverLoadedBroker = brokerConfig.overloadURI;
		

		if (isLoadAcceptingBroker)
		{
			System.out.println("<<<<<<<<<<<<< overloaded broker URI"+uriForOverLoadedBroker);
			loadAcceptanceProcess(uriForOverLoadedBroker);
		}
		running = true;
		brokerCoreLogger.info("BrokerCore is started."+this.getBrokerURI());
	}

	/**
	 * Initialize the communication layer in the connection listening mode.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initCommSystem() throws BrokerCoreException {
		// initialize the communication interface
		try {
			commSystem = createCommSystem();
			commSystem.createListener(brokerConfig.brokerURI);
			brokerCoreLogger.info("Communication System created and a listening server is initiated");
		} catch (CommunicationException e) {
			brokerCoreLogger.error("Communication layer failed to instantiate: " + e);
			exceptionLogger.error("Communication layer failed to instantiate: " + e);
			throw new BrokerCoreException("Communication layer failed to instantiate: " + e + "\t" + brokerConfig.brokerURI);
		}
	}

	/**
	 * Initialize the message queue manager which acts as a multiplexer between the communication
	 * layer and all the queues for different internal components as well as external connections.
	 * Initialize the queue manager only after initialzing the communication layer.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initQueueManager() throws BrokerCoreException {
		queueManager = createQueueManager();
		brokerCoreLogger.info("Queue Manager is created");
	}

	protected QueueManager createQueueManager() throws BrokerCoreException {
		return new QueueManager(this);
	}

	/**
	 * Initialize the router.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initRouter() throws BrokerCoreException {
		try {
			router = RouterFactory.createRouter(brokerConfig.matcherName, this);
			router.initialize();
			brokerCoreLogger.info("Router/Matching Engine is initialized");
		} catch (MatcherException e) {
			brokerCoreLogger.error("Router failed to instantiate: " + e);
			exceptionLogger.error("Router failed to instantiate: " + e);
			throw new BrokerCoreException("Router failed to instantiate: " + e);
		}
	}

	/**
	 * Initialize the input queue that is the first place a message enters from communication layer.
	 * It exploits the router to redirect traffic to different other queues.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initInputQueue() throws BrokerCoreException {
		inputQueue = createInputQueueHandler();
		inputQueue.start();
		registerQueue(inputQueue);
		brokerCoreLogger.debug("InputQueueHandler is starting.");
		try {
			inputQueue.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("InputQueueHandler failed to start: " + e);
			exceptionLogger.error("InputQueueHandler failed to start: " + e);
			throw new BrokerCoreException("InputQueueHandler failed to start", e);
		}
		brokerCoreLogger.info("InputQueueHandler is started.");
	}
	
	protected InputQueueHandler createInputQueueHandler() {
		return new InputQueueHandler(this);
	}
	
	protected Controller createController() {
		return new Controller(this);
	}
	
	protected CommSystem createCommSystem() throws CommunicationException {
		return new CommSystem();
	}


	/**
	 * Initialize the system monitor which collects broker system information. QueueManager and
	 * InputQueue must have been initialized before using this method.
	 * 
	 * @throws BrokerCoreException
	 */
	protected void initSystemMonitor() throws BrokerCoreException {
		systemMonitor = createSystemMonitor();
		System.out.println("BrokerCore >> initSystemMonitor >> System Monitor is created");
		brokerCoreLogger.info("System Monitor is created");
		// register the system monitor with queue manager and input queue, so that they can feed
		// data into the monitor
		if (queueManager == null)
			throw new BrokerCoreException(
					"QueueManager must have been initialized before SystemMonitor");
		queueManager.registerSystemMonitor(systemMonitor);
		if (inputQueue == null)
			throw new BrokerCoreException(
					"InputQueue must have been initialized before SystemMonitor");
		inputQueue.registerSystemMonitor(systemMonitor);
		systemMonitor.start();
		brokerCoreLogger.debug("System monitor is starting.");
		System.out.println("System monitor is starting.");
		try {
			systemMonitor.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("System monitor failed to start: " + e);
			exceptionLogger.error("System monitor failed to start: " + e);
			throw new BrokerCoreException("System monitor failed to start", e);
		}
		brokerCoreLogger.info("System monitor is started.");
	}
	
	protected SystemMonitor createSystemMonitor() {
		return new SystemMonitor(this);
	}

	protected void initController() throws BrokerCoreException {
		controller = createController();
		controller.start();
		brokerCoreLogger.debug("Controller is starting.");
		try {
			controller.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("Controller failed to start: " + e);
			exceptionLogger.error("Controller failed to start: " + e);
			throw new BrokerCoreException("Controller failed to start", e);
		}
		brokerCoreLogger.info("Controller is started.");
		System.out.println("Controller is started.");
	}

	protected void startMessageRateTimer() {
		ActionListener checkMsgRateTaskPerformer = new ActionListener() {

			public void actionPerformed(ActionEvent evt) {
				OverlayRoutingTable ort = getOverlayManager().getORT();
				Map<MessageDestination, LinkInfo> statisticTable = ort.getStatisticTable();
				Map<MessageDestination, OutputQueue> neighbors = ort.getBrokerQueues();
				synchronized (neighbors) {
					for (MessageDestination temp : neighbors.keySet()) {
						if (statisticTable.containsKey(temp)) {
							LinkInfo tempLink = statisticTable.get(temp);
							if (inputQueue.containsDest(temp)) {
								Integer tempI = inputQueue.getNum(temp);
								tempLink.setMsgRate(tempI.intValue());
								inputQueue.setNum(temp, new Integer(0));
							}
						}
					}
				}
			}
		};
		Timer msgRateTimer = new Timer(time_window_interval, checkMsgRateTaskPerformer);
		msgRateTimer.start();
	}

	protected void initTimerThread() throws BrokerCoreException {
		// start the timer thread (for timing heartbeats)
		timerThread = new TimerThread();
		timerThread.start();
		brokerCoreLogger.debug("TimerThread is starting.");
		try {
			timerThread.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("TimerThread failed to start: " + e);
			exceptionLogger.error("TimerThread failed to start: " + e);
			throw new BrokerCoreException("TimerThread failed to start", e);
		}
		brokerCoreLogger.info("TimerThread is started.");
	}

	protected void initHeartBeatPublisher() throws BrokerCoreException {
		// start the heartbeat publisher thread
		heartbeatPublisher = new HeartbeatPublisher(this);
		heartbeatPublisher.setPublishHeartbeats(brokerConfig.isHeartBeat());
		heartbeatPublisher.start();
		brokerCoreLogger.debug("HeartbeatPublisher is starting.");
		try {
			heartbeatPublisher.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("HeartbeatPublisher failed to start: " + e);
			exceptionLogger.error("HeartbeatPublisher failed to start: " + e);
			throw new BrokerCoreException("HeartbeatPublisher failed to start", e);
		}
		brokerCoreLogger.info("HeartbeatPublisher is started.");
	}

	protected void initHeartBeatSubscriber() throws BrokerCoreException {
		// start the heartbeat subscriber thread
		heartbeatSubscriber = createHeartbeatSubscriber();
		heartbeatSubscriber.start();
		brokerCoreLogger.debug("HeartbeatSubscriber is starting.");
		try {
			heartbeatSubscriber.waitUntilStarted();
		} catch (InterruptedException e) {
			brokerCoreLogger.error("HeartbeatSubscriber failed to start: " + e);
			exceptionLogger.error("HeartbeatSubscriber failed to start: " + e);
			throw new BrokerCoreException("HeartbeatSubscriber failed to start", e);
		}
		brokerCoreLogger.info("HeartbeatSubscriber is started.");
	}

	protected HeartbeatSubscriber createHeartbeatSubscriber() {
		return new HeartbeatSubscriber(this);
	}

	protected void initWebInterface() {
		if (brokerConfig.isWebInterface()) {
			// start the management interface web server
			webuiMonitor = new WebUIMonitor(this);
			webuiMonitor.initialize();
			brokerCoreLogger.info("ManagementInterface is started.");
		}
	}

	protected void initNeighborConnections() {
		// connect to initial remote brokers from configuration
		if (brokerConfig.getNeighborURIs().length == 0) {
			brokerCoreLogger.warn("Missing remoteBrokers key or remoteBrokers value in the property file.");
			exceptionLogger.warn("Here is an exception : ", new Exception(
					"Missing remoteBrokers key or remoteBrokers value in the property file."));
		}
		for (String neighborURI : brokerConfig.getNeighborURIs()) {
			// send OVERLAY-CONNECT(s) to controller
			Publication p = MessageFactory.createEmptyPublication();
			p.addPair("class", "BROKER_CONTROL");
			p.addPair("brokerID", getBrokerID());
			p.addPair("command", "OVERLAY-CONNECT");
			p.addPair("broker", neighborURI);
			PublicationMessage pm = new PublicationMessage(p, "initial_connect");
			if (brokerCoreLogger.isDebugEnabled())
				brokerCoreLogger.debug("Broker " + getBrokerID()
						+ " is sending initial connection to broker " + neighborURI);
			queueManager.enQueue(pm, MessageDestination.INPUTQUEUE);
		}
	}

	protected void initManagementInterface() {
		if (brokerConfig.isManagementInterface()) {
			ManagementServer managementServer = new ManagementServer(this);
			managementServer.start();
		}
	}

	protected void initConsoleInterface() {
		if (brokerConfig.isCliInterface()) {
			ConsoleInterface consoleInterface = new ConsoleInterface(this);
			consoleInterface.start();
		}
	}
	
	/**
	 * @return Initiates the load acceptance process in the new broker
	 */
	protected void loadAcceptanceProcess(String uriForOverLoadedBroker) {
		try{
		System.out.println("<<<<<<<<<<<<<<<<<<<<<<<  Subscription "
				+ "sent for newly created broker= CSStobeMigrated"+uriForOverLoadedBroker);
		String subStr = "[class,eq, CSStobeMigrated"+uriForOverLoadedBroker.replace(".", "")+"]";
		Subscription sub = MessageFactory.createSubscriptionFromString(subStr);
		System.out.println("<<<<<<<<<<<< Subscription sent for newly created broker="+sub);
		System.out.println("Sent at time ="+new Date(System.currentTimeMillis()).getHours()+":"+
				new Date(System.currentTimeMillis()).getMinutes()+":"+new Date(System.currentTimeMillis()).getSeconds());
		System.out.println("************** Message ID ="+this.getNewMessageID());
		SubscriptionMessage msg = new SubscriptionMessage(sub, this.getNewMessageID());
		msg.setPriority((short)-1);
		MessageDestination nextHopID = new MessageDestination(this.uriForOverLoadedBroker);
		nextHopID.addDestinationType(DestinationType.BROKER);
		msg.setNextHopID(nextHopID);
		System.out.println("Next hop ID #########"+msg.getNextHopID());
		System.out.println("Subscription sent is"+msg.toString());
	/*	//this.routeMessage(msg, MessageDestination.CONTROLLER);
		this.routeMessage(msg);*/
		subscribe(msg, uriForOverLoadedBroker);
		
		
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
		public SubscriptionMessage subscribe(SubscriptionMessage subMsg, String brokerURI) {		

		
		try {
			
			/*BrokerState brokerState = getBrokerState(brokerURI);
			if (brokerState == null) {
				throw new ClientException("Not connected to broker " + brokerURI);
			}
			MessageDestination clientDest = MessageDestination.formatClientDestination(clientID,
					brokerState.getBrokerAddress().getNodeURI());
			SubscriptionMessage subMsg = new SubscriptionMessage(sub,
					getNextMessageID(brokerState.getBrokerAddress().getNodeURI()), brokerURI);
			 */
			// TODO: fix this hack for historic queries
			/*Map<String, Predicate> predMap = subMsg.getSubscription().getPredicateMap();
			if (predMap.get("_start_time") != null) {
				SimpleDateFormat timeFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
				try {
					Date startTime = timeFormat.parse((String) (predMap.get("_start_time")).getValue());
					predMap.remove("_start_time");
					subMsg.setStartTime(startTime);
				} catch (java.text.ParseException e) {
					exceptionLogger.error("Fail to convert Date format : " + e);
				}
			}
			if (predMap.get("_end_time") != null) {
				SimpleDateFormat timeFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
				try {
					Date endTime = timeFormat.parse((String) (predMap.get("_end_time")).getValue());
					predMap.remove("_end_time");
					subMsg.setEndTime(endTime);
				} catch (java.text.ParseException e) {
					exceptionLogger.error("Fail to convert Date format : " + e);
				}
			}*/
			
			//brokerStates.put(brokerAddress, new BrokerState(brokerAddress));
			//BrokerState brokerState = getBrokerState(brokerURI);
			//System.out.println("############ Brokerstate for uri="+brokerURI+" is="+brokerState);
			MessageSender msgSender = commSystem.getMessageSender(brokerURI);
			msgSender.connect();
			System.out.println("********* Message sender="+msgSender.getID());
			String msgID = msgSender.send(subMsg, HostType.SERVER);
			//subMsg.setMessageID(msgID);
			//if (clientConfig.detailState)
			//	brokerState.addSubMsg(subMsg);
			
		} catch (CommunicationException e) {
			e.printStackTrace();
		}
		return subMsg;
	
	}
	
		
		public BrokerState getBrokerState(String brokerURI) {
			NodeAddress brokerAddress = null;
			try {
				brokerAddress = NodeAddress.getAddress(brokerURI);
				
			} catch (CommunicationException e) {
				e.printStackTrace();
			}
			return brokerStates.get(brokerAddress);
		}	

	/**
	 * @return The configuration of the broker
	 */
	public BrokerConfig getBrokerConfig() {
		return brokerConfig;
	}

	public WebUIMonitor getWebuiMonitor() {
		return webuiMonitor;
	}

	/**
	 * @return The
	 */
	public String getDBPropertiesFile() {
		return brokerConfig.getDbPropertyFileName();
	}

	public String getMIPropertiesFile() {
		return brokerConfig.getManagementPropertyFileName();
	}

	/**
	 * @return The ID of the broker
	 */
	public String getBrokerID() {
		return getBrokerURI();
	}

	/**
	 * @return The MessageDestination for the broker.
	 */
	public MessageDestination getBrokerDestination() {
		return brokerDestination;
	}

	/**
	 * @return
	 */
	public String getBrokerURI() {
		try {
			return commSystem.getServerURI();
	//		return NodeAddress.getAddress(brokerConfig.brokerURI).getNodeURI();
		} catch (CommunicationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

	/**
	 * Get a new (globally unique) message ID
	 * 
	 * @return The new message ID
	 */
	public synchronized String getNewMessageID() {
		return getBrokerID() + "-M" + currentMessageID++;
	}

	public MessageListenerInterface getMessageListener() {
		return queueManager;
	}

	/**
	 * Route a Message to a given destination. Errors are handled by the queueManager.
	 * 
	 * @param msg
	 *            The message to send
	 * @param destination
	 *            The destination for the message
	 */
	public void routeMessage(Message msg, MessageDestination destination) {
		System.out.println("BrokerCore >> routeMessage >> Routing to msgDestination : " + destination);
		queueManager.enQueue(msg, destination);
	}

	/**
	 * Route a Message to its nextHopID. Errors are handled by the queueManager.
	 * 
	 * @param msg
	 *            The message to send
	 */
	public void routeMessage(Message msg) {
		queueManager.enQueue(msg);
	}

	public void registerQueue(QueueHandler queue) {
		MessageQueue msgQueue = queueManager.getMsgQueue(queue.getDestination());
		if (msgQueue == null)
			queueManager.registerQueue(queue.getDestination(), queue.getMsgQueue());
		else
			queue.setMsgQueue(msgQueue);
	}

	public void registerQueue(MessageDestination msgDest, MessageQueue msgQueue) {
		queueManager.registerQueue(msgDest, msgQueue);
	}

	public void removeQueue(MessageDestination dest) {
		queueManager.removeQueue(dest);
	}

	public CommSystem getCommSystem() {
		return commSystem;
	}

	/**
	 * Get the queue for a given destination.
	 * 
	 * @param destination
	 *            The identifier for the desired queue
	 * @return The desired queue, or null if it doesn't exist
	 */
	public MessageQueue getQueue(MessageDestination destination) {
		return queueManager.getQueue(destination);
	}

	/**
	 * Get the advertisements in the broker.
	 * 
	 * @return The set of advertisements in the broker.
	 */
	public Map<String, AdvertisementMessage> getAdvertisements() {
		return router.getAdvertisements();
	}

	/**
	 * Get the subscriptions in the broker.
	 * 
	 * @return The set of subscriptions in the broker.
	 */
	public Map<String, SubscriptionMessage> getSubscriptions() {
		System.out.println("BrokerCore >> calling router getSubs");
		return router.getSubscriptions();
	}

	/**
	 * Retrieve the debug mode of this broker
	 * 
	 * @return Boolean value where true indicates debug mode
	 */
	public boolean getDebugMode() {
		return debug;
	}

	/**
	 * Set the debug mode of this broker
	 * 
	 * @param debugMode
	 *            True to set broker to debug mode, false to turn off debug mode
	 */
	public void setDebugMode(boolean debugMode) {
		debug = debugMode;
	}

	/**
	 * Returns the number of messages in the input queue
	 * 
	 * @return the number of messages in the input queue
	 */
	public int getInputQueueSize() {
		return inputQueue.getInputQueueSize();
	}

	/**
	 * Shuts down this broker along with all services under this broker
	 */
	public void shutdown() {
		
		if(isShutdown)
			return;
		
		isShutdown  = true;
		systemMonitor.shutdownBroker();
		
		// Let's be nice
		try {
//			stop();
			brokerCoreLogger.info("BrokerCore is shutting down.");
//			orderQueuesTo("SHUTDOWN");
			if (commSystem != null)
				commSystem.shutDown();
		} catch (CommunicationException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
		
		controller.shutdown();
		inputQueue.shutdown(); 
		timerThread.shutdown(); 
		heartbeatPublisher.shutdown();
		heartbeatSubscriber.shutdown();	
	}

	/**
	 * Stops all broker activity Publishers/Neighbours can still send messages to the brokercore
	 */
	public void stop() {
		// Stop all input/output queues from receiving messages.
		// NOTE: The input queue is never stopped or else there will be no way to start it up again
		// remotely
		try {
			brokerCoreLogger.info("BrokerCore is stopping.");
			orderQueuesTo("STOP");
			running = false;
		} catch (ParseException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
	}

	/**
	 * Resumes all broker activity
	 * 
	 */
	public void resume() {
		// Allow messages to be delivered
		try {
			brokerCoreLogger.info("BrokerCore is resuming.");
			orderQueuesTo("RESUME");
			running = true;
		} catch (ParseException e) {
			e.printStackTrace();
			exceptionLogger.error(e.getMessage());
		}
	}

	/*
	 * Send a STOP, RESUME, or SHUTDOWN control message to the LifeCycle, Overlay Managers and System Monitor
	 */
	protected void orderQueuesTo(String command) throws ParseException {
		// Send a control message to the LifeCycle Manager
		Publication lcPub = MessageFactory.createPublicationFromString("[class,BROKER_CONTROL],[brokerID,'" + getBrokerID()
				+ "'],[command,'LIFECYCLE-" + command + "']");
		PublicationMessage lcPubmsg = new PublicationMessage(lcPub, getNewMessageID(),
				getBrokerDestination());
		brokerCoreLogger.debug("Command " + command + " is sending to LifecycleManager.");
		if (queueManager != null)
			queueManager.enQueue(lcPubmsg, MessageDestination.CONTROLLER);

		// Send a control message to the Overlay Manager
		Publication omPub = MessageFactory.createPublicationFromString("[class,BROKER_CONTROL],[brokerID,'" + getBrokerID()
				+ "'],[command,'OVERLAY-" + command + "']");
		PublicationMessage omPubmsg = new PublicationMessage(omPub, getNewMessageID(),
				getBrokerDestination());
		brokerCoreLogger.debug("Command " + command + " is sending to OverlayManager.");
		if (queueManager != null)
			queueManager.enQueue(omPubmsg, MessageDestination.CONTROLLER);
	}

	/**
	 * Indicates whether this broker is running or not
	 * 
	 * @return boolean value, true indicates the broker is running, false means the broker is
	 *         stopped.
	 */
	public boolean isRunning() {
		return running;
	}

	public CycleType getCycleOption() {
		return brokerConfig.getCycleOption();
	}

	public boolean isDynamicCycle() {
		return isDynamicCycle;
	}

	public boolean isCycle() {
		return isCycle;
	}

	public SystemMonitor getSystemMonitor() {
		if (systemMonitor == null) {
			System.err.println("Call to getSystemMonitor() before initializing the system monitor");
		}
		return systemMonitor;
	}

	public Controller getController() {
		return controller;
	}

	public OverlayManager getOverlayManager() {
		return controller == null ? null : controller.getOverlayManager();
	}

	public HeartbeatPublisher getHeartbeatPublisher() {
		return heartbeatPublisher;
	}

	public TimerThread getTimerThread() {
		return timerThread;
	}

	public Router getRouter() {
		return router;
	}

	public InputQueueHandler getInputQueue() {
		return inputQueue;
	}

	public static void main(String[] args) {
		try {
			BrokerCore brokerCore = new BrokerCore(args);
			brokerCore.initialize();
//			brokerCore.shutdown();
		} catch (Exception e) {
			// log the error the system error log file and exit
			Logger sysErrLogger = Logger.getLogger("SystemError");
			if (sysErrLogger != null)
				sysErrLogger.fatal(e.getMessage() + ": " + e);
			e.printStackTrace();
			System.exit(1);
		}
	}

	public boolean isShutdown() {
		return isShutdown;
	}
	
	public static void startBroker(String[] args)
	{
		try {
			BrokerCore brokerCore = new BrokerCore(args);
			brokerCore.initialize();
//			brokerCore.shutdown();
		} catch (Exception e) {
			// log the error the system error log file and exit
			Logger sysErrLogger = Logger.getLogger("SystemError");
			if (sysErrLogger != null)
				sysErrLogger.fatal(e.getMessage() + ": " + e);
			e.printStackTrace();			
		}
	}

}
