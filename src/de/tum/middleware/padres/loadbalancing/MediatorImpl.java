package loadBalancing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.util.CommandLine;

public class MediatorImpl extends Client implements Runnable
{
	public HashMap<String, HashMap<String, String>> brokerMap = new HashMap<>();
	
	protected static final String CONFIG_FILE_PATH = String.format(
			"%s/etc/guiclient/client.properties", ClientConfig.PADRES_HOME);

	public MediatorImpl() throws ClientException{
		// TODO Auto-generated constructor stub
		super("Mediator");
	}
	
	public MediatorImpl(ClientConfig userConfig) throws ClientException{
		// TODO Auto-generated constructor stub
		super(userConfig);
	}

	@Override
	public void processMessage(Message msg) {
		// TODO Auto-generated method stub
		HashMap<String, String> brokerData = new HashMap<>();
		super.processMessage(msg);
		System.out.println("Mediator >> processMessage >> Message Recevied : " + msg);
		
		int startIndex = msg.toString().indexOf("[");
		int endIndex = msg.toString().lastIndexOf("]");
		String trimmedStr = msg.toString().substring(startIndex,endIndex+1);
		
		do {
		int start = trimmedStr.indexOf("[");
		int end = trimmedStr.indexOf("]");
		String keyValue = trimmedStr.substring(start+1, end);
		String words[] = keyValue.split(",");
		brokerData.put(words[0], words[1]);
		trimmedStr = trimmedStr.substring(end+1,trimmedStr.length());
		}
		while(trimmedStr.length() != 0);
		//System.out.println("MediatorImpl >> brokerData : " + brokerData);
		
		String brokerID = brokerData.get("brokerID");
		System.out.println("brokerID : " + brokerID);
		if(brokerMap.containsKey(brokerID))
			brokerMap.remove(brokerID);
		brokerMap.put(brokerID, brokerData);
		System.out.println("\nMediatorImpl >> brokerMap : " + brokerMap + "\n");
		
		//Determine the overloaded broker
		String overloadedBrokerID =  getOverloadedBroker(brokerMap);
		System.out.println("Overloaded broker : " + overloadedBrokerID);
		if(overloadedBrokerID != null)
		{
			
			/*
			BrokerCore brokerCore;
			try {
				brokerCore = new BrokerCore("-uri socket://localhost:1126/BrokerZ");
				brokerCore.initialize();
			} catch (BrokerCoreException e) {
				e.printStackTrace();
			}
			*/
		}
	}
	
	/**
	 * This function analyzes the information sent by all brokers and returns the broker which is overloaded
	 * @return
	 */
	public String getOverloadedBroker(HashMap<String, HashMap<String, String>> brokerMap) {
		/* check performance metrics and STATUS of every broker.
		Change STATUS to "NA" if broker id overloaded. 
		Broker will only be considered overloaded if STATUS is "NA".
		*/
		Iterator<Map.Entry<String, HashMap<String, String>>> iterator = brokerMap.entrySet().iterator();
		while(iterator.hasNext())
		{
			Map.Entry<String, HashMap<String, String>> entry = iterator.next();
			calculatePerformance(entry);
		}
		Iterator<Map.Entry<String, HashMap<String, String>>> it = brokerMap.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<String, HashMap<String, String>> entry = it.next();
			if(brokerMap.get(entry.getKey()).get("STATUS").contains("NA"))
			{
				return entry.getKey();
			}
		}
		return null;
	}
	
	/**
	 * This function calculates performance metrics of each broker in brokerMap 
	 * and sets the STATUS of overloaded broker
	 * @param entry
	 */
	public void calculatePerformance(Map.Entry<String, HashMap<String, String>> entry) {
		System.out.println("Calculating Performance Mentrics of broker : " + entry.getKey());
		HashMap<String, String> brokerDataMap = brokerMap.get(entry.getKey());
		String numberOfNeighbours = brokerDataMap.get("numberOfNeighbours");
		numberOfNeighbours = numberOfNeighbours.replaceAll("[^0-9]", "");
		
		if(Integer.parseInt(numberOfNeighbours) > 1)
		{
			System.out.println("Number of neighbours : " + numberOfNeighbours);
			System.out.println("OVERLOADED....!!!!");
			brokerDataMap.put("STATUS", "NA");
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}

	public static void main(String[] args) {

		try {
			CommandLine cmdLine = new CommandLine(ClientConfig.getCommandLineKeys());
			cmdLine.processCommandLine(args);
			String configFile = cmdLine.getOptionValue(ClientConfig.CLI_OPTION_CONFIG_FILE,
					CONFIG_FILE_PATH);
			System.out.println("Mediator >> main >> configFile : " + configFile);
			// load the client configuration
			ClientConfig userConfig = new ClientConfig(configFile);
			userConfig.overwriteWithCmdLineArgs(cmdLine);

			MediatorImpl mediator = new MediatorImpl(userConfig);
			System.out.println("Mediator created : " + mediator.clientID);
			mediator.subscribe(MessageFactory.createSubscriptionFromString("[class,eq,BROKER_INFO]"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}