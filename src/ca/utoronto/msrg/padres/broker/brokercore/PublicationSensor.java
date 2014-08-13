package ca.utoronto.msrg.padres.broker.brokercore;

public class PublicationSensor extends Thread {
	
	BrokerCore broker;
	public PublicationSensor(BrokerCore broker) {
		this.broker = broker;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		super.run();
		System.out.println("PublicationSensor started for broker : " + this.broker);
		try {
			this.broker.queueManager.setRecordPublication(true);
			System.out.println("Publication Sensor going to sleep.........*********");
			Thread.sleep(8000);
			System.out.println("Publication Sensor waking up.........*********");
			this.broker.queueManager.setRecordPublication(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
