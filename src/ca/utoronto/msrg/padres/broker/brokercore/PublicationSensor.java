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
			QueueManager queueManager = new QueueManager(this.broker);
			queueManager.setRecordPublication(true);
			Thread.sleep(8000);
			queueManager.setRecordPublication(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
