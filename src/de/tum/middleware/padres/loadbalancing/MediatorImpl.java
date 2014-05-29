package de.tum.middleware.padres.loadbalancing;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.comm.CommSystem;

public class MediatorImpl extends Client implements Runnable
{
	
	public MediatorImpl() throws ClientException{
		// TODO Auto-generated constructor stub
		super("Mediator");
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args) {
		
		try {
			MediatorImpl mediator = new MediatorImpl();
			String uri ="";
			CommSystem commSys = new CommSystem();
			commSys.createListener(uri);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
