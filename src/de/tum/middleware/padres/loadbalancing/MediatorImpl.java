package de.tum.middleware.padres.loadbalancing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
		System.out.println("\nMediatorImpl >> brokerMap : " + brokerMap + "\n");
		initiateOverloadingProcess();
		
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
	 * This function determines the overloaded broker
	 * @return
	 */

	public static void initiateOverloadingProcess()
	{
		try{
			List <String> overloadedBrokerID =  getOverloadedBroker();
			for (String currBroker : overloadedBrokerID)
			{
				HashMap <String, String> tempBroker = brokerMap.get(currBroker);
				System.out.println("The neighbors are ="+tempBroker.get("NEIGHBORS"));
				sshCallToHost(tempBroker.get("NEIGHBORS"), currBroker);
			}
			
		}catch (Exception e)
		{
			e.printStackTrace();			
		}
	}
	
	/**
	 * This function analyzes the information sent by all brokers and returns the broker which is overloaded
	 * @return
	 */
	public static List<String> getOverloadedBroker() {
		
		/* Check performance metrics and STATUS of every broker.
		Change STATUS to "NA" if broker id overloaded. 
		Broker will only be considered overloaded if STATUS is "NA".
		*/
		
		float threshold = 0.0f;
		List<String> overloadedBroker = new ArrayList<String>();
		try {
			Iterator<Map.Entry<String, HashMap<String, String>>> iterator = brokerMap.entrySet().iterator();
			float maxIR = 0f;			
			while(iterator.hasNext())
			{
				Map.Entry<String, HashMap<String, String>> entry = iterator.next();			
				float currIR = calculateIRPerformance(entry);
				System.out.println(" decision ="+(maxIR <= currIR));
				if (currIR <= threshold)
				{				
					System.out.println("Broker ID ="+entry.getKey());
					System.out.println("Value of Ir ="+maxIR);
					
					HashMap <String, String> temp = brokerMap.get(entry.getKey());
					System.out.println("Current broker to change the status to NA ="+temp);
					 
					if ("OK".equals(temp.get("STATUS")))
					{
						overloadedBroker.add(entry.getKey());
						//temp.remove("STATUS");
						//temp.put("STATUS", "NA");
					}
					else
					{
						return overloadedBroker;
					}
					
				}				
			}
				
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
				ir = incomingPubMsgRate * averageMatchTime;
			
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
			/*while (true) {
				Thread.sleep(45000);
				String overloadedBrokerID = getOverloadedBroker(brokerMap);
				String [] neighbors = new String [2];
				//New broker 
				sshCallToHost(neighbors, overloadedBrokerID);
				System.out.println("the overloaded broker = "
						+ overloadedBrokerID);
			}*/
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * This function connects to the new system and initiates the BrokerCore.java
	 * @param entry
	 */
	
	private static boolean sshCallToHost(String neighbors, String overloadBrkUri)
	{	
		System.out.println("inside sshCallToHost");
		String dir = System.getProperty("user.dir")+"/etc/scripts/instantiate_server.sh";
		String uriLoadAcceptingBrk = "";
		boolean result = false;
		Process proc = null;
		
		if (overloadBrkUri.contains("socket"))
		{
			uriLoadAcceptingBrk = getAvailableBrokerFromFile("socket");
		}
		else
		{
			uriLoadAcceptingBrk = getAvailableBrokerFromFile("rmi");
		}
		
		
		if ( uriLoadAcceptingBrk== null || "".equalsIgnoreCase(uriLoadAcceptingBrk))
		{
			System.out.println(" There are no available systems to start loadbalancing");
			return false;
		}
		else
		{
			System.out.println("The participating broker is "+uriLoadAcceptingBrk);			
		}
		
		if (neighbors==null || "".equalsIgnoreCase(neighbors))
		{
			neighbors = new String(overloadBrkUri);
		}
		else
		{
			neighbors = neighbors + "," + overloadBrkUri;
		}
			
		
		//cmd[2] = dir+script;
		//cmd[3] = address;
		//cmd[4] = port+"";

				
		Runtime run = Runtime.getRuntime();

		try {			
				System.out.println("testRun >> ovl broker : " + overloadBrkUri + " **** new broker : " + uriLoadAcceptingBrk +" **** neighbors:"+neighbors);
				String cmd1 = new String (dir+" "+overloadBrkUri+" "+uriLoadAcceptingBrk+" "+neighbors);
				System.out.println("script to run ******"+cmd1);
				proc = run.exec(cmd1);
				proc.waitFor();
				String output = readStream(proc.getInputStream());
				String error = readStream(proc.getErrorStream());
				System.out.println(" Input Stream = " + output.trim());
				System.out.println(" Error Stream = " + error.trim());
				/*System.out.println(" Input Stream = " + output);
				System.out.println(" Error Stream = " + error.trim());*/
				if (null==error || "".equals(error))
				{
					result = true;
					System.out.println("Server "+uriLoadAcceptingBrk+" started properly!!! \n ");
				}
				else{
					System.out.println("Server "+uriLoadAcceptingBrk+" could not start properly!!! \n "+error);
				}
			} catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
//			if (proc!=null)
//			{
//				proc.destroy();
//			}
		}
		return result;
	}
	
	private static String getAvailableBrokerFromFile(String type)
	{
		String brokerUri = "";
		BufferedReader br = null;
		BufferedWriter bw = null;
		try {
			br = new BufferedReader( new FileReader (System.getProperty("user.dir")+"/etc/mediator/availablebrokers.properties"));
			String tempStr ="", finalStr = "";
			boolean matched = true;
			while ((tempStr = br.readLine())!=null)
			{				
				if (tempStr.trim().substring(0, 1).equals("#"))
				{
					finalStr = finalStr + tempStr + "\n";
					continue;
				}
				String tempStrArr [] = tempStr.split(",");
				if (tempStrArr[2].equalsIgnoreCase("Available") && matched)
				{
					finalStr = finalStr + tempStrArr[0]+","+tempStrArr[1]+",Working\n";
					brokerUri = type+"://"+tempStrArr[0]+"/"+tempStrArr[1];
					matched = false;
				}
				else
				{
					finalStr = finalStr + tempStr + "\n";
				}				
			}
			
			bw = new BufferedWriter(new FileWriter(System.getProperty("user.dir")+"/etc/mediator/availablebrokers.properties"));
			bw.write(finalStr);
			bw.flush();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			try {
				br.close();
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("available broker from file "+brokerUri);
		return brokerUri;
	}
	
	private static String readStream(InputStream in) {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		StringBuffer sb = new StringBuffer();
		try {
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//logger.error(e);
		}
		return sb.toString();
	}
}