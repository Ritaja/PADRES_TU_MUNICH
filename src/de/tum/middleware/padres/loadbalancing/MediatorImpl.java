package loadBalancing;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.comm.CommSystem;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.tools.guiclient.GUIClient;

public class MediatorImpl extends Client implements Runnable
{
	
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
		super.processMessage(msg);
		System.out.println("Mediator >> processMessage >> Message Recevied : " + msg);
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
			mediator.subscribe(MessageFactory.createSubscriptionFromString("[class,eq,'temp']"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}