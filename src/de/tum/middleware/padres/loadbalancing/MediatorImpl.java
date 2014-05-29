package de.tum.middleware.padres.loadbalancing;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.comm.CommSystem;

public class MediatorImpl extends Client implements Runnable{
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	public MediatorImpl()  throws ClientException
	{
		// TODO Auto-generated constructor stub		
		super("Mediator");		
	}
	
	public static void main(String[] args) {
		try {
			String uri = "socket://localhost:1101/BrokerA";
			CommSystem commSystem = new CommSystem();
			commSystem.createListener(uri);
			MediatorImpl mediator = new MediatorImpl();					
			mediator.connect(uri);
			//mediator.subscribe(MessageFactory.createSubscriptionFromString("[class,eq,BROKER_INFO]"));
			/*CommSystem commSystem = new CommSystem();
			commSystem.createListener(uri);
			commSystem.*/

			
			//mediator.processMessage(msg);
		
		} catch (ClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
