package de.tum.middleware.padres.loadbalancing;

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
	public static HashMap<String, HashMap<String, String>> brokerMap = new HashMap<String, HashMap<String, String>>();
	
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
		HashMap<String, String> brokerData = new HashMap<String, String>();
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
		//System.out.println("\nMediatorImpl >> brokerMap : " + brokerMap + "\n");
		
		//Determine the overloaded broker
		//String overloadedBrokerID =  getOverloadedBroker(brokerMap);
		//System.out.println("Overloaded broker : " + overloadedBrokerID);
		/*
		if(overloadedBrokerID != null)
		{
			
	
			BrokerCore brokerCore;
			try {
				brokerCore = new BrokerCore("-uri socket://localhost:1126/BrokerZ");
				brokerCore.initialize();
			} catch (BrokerCoreException e) {
				e.printStackTrace();
			}
			
		}
		*/
	}
	
	/**
	 * This function analyzes the information sent by all brokers and returns the broker which is overloaded
	 * @return
	 */
	public static String getOverloadedBroker(HashMap<String, HashMap<String, String>> brokerMap) {
		/* check performance metrics and STATUS of every broker.
		Change STATUS to "NA" if broker id overloaded. 
		Broker will only be considered overloaded if STATUS is "NA".
		*/
		String overloadedBroker = "";
		try {
			Iterator<Map.Entry<String, HashMap<String, String>>> iterator = brokerMap.entrySet().iterator();
			float maxIR = 0f;
			while(iterator.hasNext())
			{
				Map.Entry<String, HashMap<String, String>> entry = iterator.next();			
				float currIR = calculateIRPerformance(entry);
				System.out.println(" decision ="+(maxIR <= currIR));
				if (maxIR <= currIR)
				{
					maxIR = currIR;
					System.out.println("Broker ID ="+entry.getKey());
					System.out.println("Value of Ir ="+maxIR);
					overloadedBroker = entry.getKey();
				}				
			}
			HashMap <String, String> temp = brokerMap.get(overloadedBroker);
			System.out.println("temp ="+temp);
			if (temp!=null && temp.containsKey("STATUS"))
				temp.remove("STATUS");
			temp.put("STATUS", "NA");
			/*Iterator<Map.Entry<String, HashMap<String, String>>> it = brokerMap.entrySet().iterator();
			while(it.hasNext())
			{
				Map.Entry<String, HashMap<String, String>> entry = it.next();
				if(brokerMap.get(entry.getValue()).get("STATUS").contains("NA"))
				{
					return entry.getKey();
				}		
			}*/
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return overloadedBroker;
	}
	
	/**
	 * This function calculates performance metrics of each broker in brokerMap 
	 * and returns the IR for the broker
	 * @param entry
	 */
	public static float calculateIRPerformance(Map.Entry<String, HashMap<String, String>> entry) {
		float ir = 0f;
		try {
			System.out.println("Calculating IR Performance Metrics of broker : " + entry.getKey());
			HashMap<String, String> brokerDataMap = brokerMap.get(entry.getKey());
			/*String numberOfNeighbours = brokerDataMap.get("numberOfNeighbours");
			numberOfNeighbours = numberOfNeighbours.replaceAll("[^0-9]", "");
			
			if(Integer.parseInt(numberOfNeighbours) > 1)
			{
				System.out.println("Number of neighbours : " + numberOfNeighbours);
				System.out.println("OVERLOADED....!!!!");
				brokerDataMap.put("STATUS", "NA");
			}*/
			
			float incomingPubMsgRate = Float.parseFloat(brokerDataMap.get("incomingPubMsgRate")
					.substring(1, brokerDataMap.get("incomingPubMsgRate").length()-1));
			float averageMatchTime = Float.parseFloat(brokerDataMap.get("averageMatchTime").
					substring(1,brokerDataMap.get("averageMatchTime").length()-1));
			if (averageMatchTime == 0)
				ir = 0;
			else			
				ir = incomingPubMsgRate / averageMatchTime;
			
			System.out.println("Value for IR in the calculation is "+incomingPubMsgRate +"    "+averageMatchTime);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return ir;
		
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
			while (true) {
				Thread.sleep(30000);
				String overloadedBrokerID = getOverloadedBroker(brokerMap);
				System.out.println("the overloaded broker = "
						+ overloadedBrokerID);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}