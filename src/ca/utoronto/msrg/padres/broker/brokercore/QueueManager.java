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
 * Created on 17-Jul-2003
 *
 */
package ca.utoronto.msrg.padres.broker.brokercore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.broker.controller.OverlayManager;
import ca.utoronto.msrg.padres.broker.monitor.SystemMonitor;
import ca.utoronto.msrg.padres.common.comm.CommSystem;
import ca.utoronto.msrg.padres.common.comm.CommSystem.HostType;
import ca.utoronto.msrg.padres.common.comm.MessageListenerInterface;
import ca.utoronto.msrg.padres.common.comm.MessageQueue;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.MessageType;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.Unsubscription;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;

/**
 * The QueueManager handles the collection of MessageQueues for all destinations
 * in the broker.
 * 
 * @author eli
 */
public class QueueManager implements MessageListenerInterface {

	protected BrokerCore brokerCore;

	protected Map<MessageDestination, MessageQueue> queues;

	protected SystemMonitor systemMonitor;

	private boolean recordPublication = false;

	static Logger exceptionLogger = Logger.getLogger("Exception");

	static Logger messagePathLogger = Logger.getLogger("MessagePath");

	public QueueManager(BrokerCore broker) throws BrokerCoreException {
		brokerCore = broker;
		queues = new HashMap<MessageDestination, MessageQueue>();
		CommSystem commSystem = broker.getCommSystem();
		if (commSystem == null)
			throw new BrokerCoreException(
					"Instantiating QueueManager before initialzing the CommSystem is not allowed");
		commSystem.addMessageListener(this);
	}

	public void registerSystemMonitor(SystemMonitor systemMonitor) {
		this.systemMonitor = systemMonitor;
	}

	public void registerQueue(MessageDestination msgDest, MessageQueue msgQueue) {
		synchronized (queues) {
			queues.put(msgDest, msgQueue);
		}
	}

	public void removeQueue(MessageDestination dest) {
		synchronized (queues) {
			queues.remove(dest);
		}
	}

	public void deleteQueue(MessageDestination destination) {
		synchronized (queues) {
			if (queues.containsKey(destination) == false) {
				// TODO: handle invalid queue
			} else {
				queues.remove(destination);
			}
		}
	}

	public boolean isRecordPublication() {
		return recordPublication;
	}

	public void setRecordPublication(boolean recordPublication) {
		this.recordPublication = recordPublication;
	}

	/**
	 * Get the queue for a given destination.
	 * 
	 * @param destination
	 *            The identifier for the desired queue
	 * @return The desired queue, or null if it doesn't exist
	 */
	public MessageQueue getQueue(MessageDestination destination) {
		if (queues.containsKey(destination) == true) {
			return (MessageQueue) queues.get(destination);
		} else {
			return null;
		}
	}

	/**
	 * Enqueue a message in the queue for the given destination. If no queue
	 * exists for the exact destination, components will be removed until either
	 * a queue is found or all components are removed.
	 * 
	 * @param msg
	 * @param destination
	 */
	public synchronized void enQueue(Message msg, MessageDestination destination) {
		msg.setNextHopID(destination);
		MessageQueue queue = getMsgQueue(destination);
		if (queue == null) {
			// System.out.println("QueueManager>>enQueue::destination "+destination.isBroker());
			// System.out.println("QueueManager>>enQueue::destination "+destination.isInternalQueue());
			if (destination.isBroker()) {
				System.out
						.println("QueueHandler>>enQueue>>BROKER DESTINATION:: "
								+ destination);
				if (destination.equals(brokerCore.getBrokerDestination())) {
					return;
				} else {
					// Handle invalid destination by creating a queue.
					// Presumably, a handler will later be created for this
					// queue.
					queue = createMessageQueue();
					System.out
							.println("QueueHandler>>enQueue>>BROKER DESTINATION:: "
									+ destination + "Queue:: " + queue);
					registerQueue(destination, queue);
					// queue.notifyAll();
				}
			} else {
				messagePathLogger.fatal("QueueManager: queue for "
						+ destination + " not found. Msg is " + msg);
				exceptionLogger.fatal("Here is an exception: ", new Exception(
						"QueueManager: queue for " + destination
								+ " not found. Msg is " + msg));
				return;
			}
		}

		// ====================== MONITOR STUFF ===============================
		if (systemMonitor.stopTracerouteMsgDelivery(msg))
			return;
		// Process incoming messages to this broker
		if (destination == MessageDestination.INPUTQUEUE) {
			// Count rate of messages processed
			systemMonitor.countMessage(msg);
		}
		// ====================================================================

		// messages that are not publication messages are not sent to clients
		OverlayManager overlayManager = brokerCore.getOverlayManager();
		if (overlayManager != null
				&& overlayManager.getORT().isClient(destination)) {
			if (!(msg instanceof PublicationMessage)) {
				messagePathLogger.warn("The incoming message for client "
						+ msg.getNextHopID() + " is not a publication.");
				exceptionLogger.warn("Here is an exception : ", (new Exception(
						"The incoming message for client " + msg.getNextHopID()
								+ " is not a publication.")));
				// don't queue it
				return;
			}
		}
		// place the message into the designated queue so that the relevant
		// queue handler can
		// process it in due time
		queue.add(msg);
	}

	protected MessageQueue createMessageQueue() {
		return new MessageQueue();
	}

	public MessageQueue getMsgQueue(MessageDestination destination) {
		MessageQueue queue = queues.get(destination);
		MessageDestination tempDestination = destination.removeComponent();
		while (queue == null && tempDestination != null) {
			queue = queues.get(tempDestination);
			tempDestination = tempDestination.removeComponent();
		}
		return queue;
	}

	/**
	 * Enqueue a message in the queue for its nextHopID. If no queue exists for
	 * the exact destination, components will be removed until either a queue is
	 * found or all components are removed.
	 * 
	 * @param msg
	 */
	public void enQueue(Message msg) {
		System.out.println("QueueManager>>enQueue:: " + msg + "nexthopID:: "
				+ msg.getNextHopID());
		enQueue(msg, msg.getNextHopID());
	}

	@Override
	public void notifyMessage(Message msg, HostType sourceType) {
		boolean dropped = false;
		System.out.println("QueueManager >> notifyMessage >> HostType : "
				+ sourceType);
		System.out.println("QueueManager >> notifyMessage >> Message Type : "
				+ msg.getType());
		System.out
				.println("QueueManager >> notifyMessage >> isRecordPublication : "
						+ isRecordPublication());

		if (sourceType == HostType.SERVER
				&& msg.getType() == MessageType.SUBSCRIPTION) {
			System.out.println("QueueHandler>>RECIEVED SUBSCRIPTION: "
					+ ((SubscriptionMessage) msg).getSubscription()
							.getClassVal());
		}
		if (sourceType == HostType.SERVER) {
			// The broker should not receive advertisement again, which is sent
			// by this broker
			// before. To avoid the advertisement loop in the cyclic network
			dropped = (msg.getType() == MessageType.ADVERTISEMENT)
					&& msg.getMessageID().startsWith(
							brokerCore.getBrokerDestination() + "-M");
		} else {
			msg.setMessageID(brokerCore.getNewMessageID());
			if (msg.getType() == MessageType.SUBSCRIPTION) {
				System.out
						.println("================ SUBSCRIPTION MESSAGE RECEIVED =====================");
				System.out
						.println("<<<<<<<< QueueManager --- notifyMessage ----- Subscription||"
								+ msg.toString());
				SubscriptionMessage subMsg = (SubscriptionMessage) msg;
				// TODO: fix this hack for historic queries
				Map<String, Predicate> predMap = subMsg.getSubscription()
						.getPredicateMap();
				if (predMap.get("_start_time") != null) {
					SimpleDateFormat timeFormat = new SimpleDateFormat(
							"EEE MMM dd HH:mm:ss zzz yyyy");
					try {
						Date startTime = timeFormat.parse((String) (predMap
								.get("_start_time")).getValue());
						predMap.remove("_start_time");
						subMsg.setStartTime(startTime);
					} catch (ParseException e) {
						exceptionLogger.error("Fail to convert Date format : "
								+ e);
					}
				}
				if (predMap.get("_end_time") != null) {
					SimpleDateFormat timeFormat = new SimpleDateFormat(
							"EEE MMM dd HH:mm:ss zzz yyyy");
					try {
						Date endTime = timeFormat.parse((String) (predMap
								.get("_end_time")).getValue());
						predMap.remove("_end_time");
						subMsg.setEndTime(endTime);
					} catch (ParseException e) {
						exceptionLogger.error("Fail to convert Date format : "
								+ e);
					}
				}
			}
		}
		if (!dropped) {
			enQueue(msg, MessageDestination.INPUTQUEUE);
		}

		System.out.println("QueueManager >> isRecordPublication : "
				+ isRecordPublication());

		if (msg.getType().equals(MessageType.PUBLICATION)
				&& isRecordPublication()) {
			System.out
					.println("QueueManager >> notifyMessage >> ((PublicationMessage) msg class : "
							+ ((PublicationMessage) msg).getPublication()
									.getClassVal());
			brokerCore.notifyBroker((PublicationMessage) msg);
		}

		if (msg.getType().equals(MessageType.PUBLICATION)) {

			String msgStr = msg.toString();
			System.out
					.println("<<<<<<<< QueueManager --- notifyMessage ----- Publication||"
							+ msgStr);

			if ((((PublicationMessage) msg).getPublication().getClassVal())
					.contains("CSStobeMigrated")
					&& this.brokerCore.isLoadAcceptingBroker()) {
				System.out
						.println("QueueManager >> notifyMessage >> ((CSStobeMigrated PublicationMessage) msg class : "
								+ ((PublicationMessage) msg).getPublication()
										.getClassVal());
				brokerCore.subscribeCSStoMigrate((PublicationMessage) msg);
			}
				if ((((PublicationMessage) msg).getPublication().getClassVal())
						.contains("CSStobeMigratedACK")) {
					System.out.println("QueueManager>>CSStobeMigratedACK: "+msgStr);
					brokerCore.unsubscribeSubscriptions();
				    System.out.println("QueueManager>>NotifyMessage:: Final subscription list::  "+brokerCore.getSubscriptions());
				    this.brokerCore.setStatus("LOADBALANCE_COMPLETE");
				    this.brokerCore.systemMonitor.forcePublishBrokerInfo();
				    this.brokerCore.setStatus("OK");
				    this.brokerCore.systemMonitor.forcePublishBrokerInfo();
			}
		}
		
		if (msg.getType().equals(MessageType.ADVERTISEMENT)) {
			System.out.println("QueueManager >> notifyMessage >> Advertisement msg class : "+((AdvertisementMessage) msg).toString());
		}
		
		if (msg.getType().equals(MessageType.SUBSCRIPTION)) {
			System.out
					.println("Queue manager---notifyMessage----Subscription received="
							+ msg.toString());

			System.out.println("((SubscriptionMessage) msg class : "
					+ ((SubscriptionMessage) msg).getSubscription()
							.getClassVal());
			String CSScompare = "CSStobeMigrated"
					+ this.brokerCore.getBrokerURI().replace(".", "");

			System.out.println("Subscriptions:::::::::: "
					+ ((SubscriptionMessage) msg).getSubscription());
			String accepterURI = null;

			if (((SubscriptionMessage) msg).getSubscription().getClassVal()
					.equals(CSScompare)) {
				accepterURI = (((SubscriptionMessage) msg).getSubscription()
						.getPredicateMap().toString()).substring(13);

				String newBrokerURIArr[] = accepterURI.split(","); 
				System.out
						.println("<<<<<<<<<< Value for Accepter predicate::: "
								+ newBrokerURIArr[0]);
				if ("LOADBALANCE_COMPLETE".equalsIgnoreCase(newBrokerURIArr[0])) {/*
					
					 * This is the case when CSStobeMigrated subscription
					 * appears for the 2nd time when the load balance process is
					 * over. Here unsubscription happens.
					 

					// Send unsubscribe message for the CSS classes which were
					// migrated.

					System.out.println("QueueManager >> notifyMessage >>> LOADBALANCE_COMPLETE");

					Map<String, SubscriptionMessage> subs = this.brokerCore
							.getSubscriptions();
					System.out
							.println("QueueManager >> notifyMessage >> subscriptions retrieved :"
									+ subs);

					List<String> subscriptionList = new ArrayList<String>();

					Iterator<Map.Entry<String, SubscriptionMessage>> it = subs
							.entrySet().iterator();
					while (it.hasNext()) {

						Entry<String, SubscriptionMessage> thisEntry = it
								.next();
						String brokerInfoMsg = thisEntry.getValue()
								.getSubscription().toString();
						String brokerInfoMsgClass = thisEntry.getValue()
								.getSubscription().getClassVal();

						if (brokerInfoMsgClass
								.equalsIgnoreCase("HEARTBEAT_MANAGER")
								|| brokerInfoMsgClass
										.equalsIgnoreCase("NETWORK_DISCOVERY")
								|| brokerInfoMsgClass
										.equalsIgnoreCase("BROKER_INFO")
								|| brokerInfoMsgClass
										.equalsIgnoreCase("GLOBAL_FD")
								|| brokerInfoMsgClass
										.equalsIgnoreCase("BROKER_CONTROL")
								|| brokerInfoMsgClass
										.equalsIgnoreCase("CSStobeMigrated"
												+ this.brokerCore
														.getBrokerURI()
														.replace(".", ""))
								|| brokerInfoMsgClass
										.equalsIgnoreCase("BROKER_MONITOR")) {

							System.out
									.println("QueueManager >> Control Messages skipped"
											+ brokerInfoMsgClass);

						} else {
							// Unsubscription process starts
							
							String classesToUnSub = this.brokerCore
									.getClassesTransferred();
							String classestoUnSubArr[] = classesToUnSub
									.split(" ");
							for (String subClass : classestoUnSubArr) {
								if (brokerInfoMsg.contains(subClass)) {
									System.out.println("QueueManager <<<< notifyMessage <<<< Unsubscription is done for message="+brokerInfoMsg);
									// Unsubscription unsub =
									 //MessageFactory.createSubscriptionFromString(brokerInfoMsg);
									// Unsubscription Process has to happen	
								}
							}

							subscriptionList.add(thisEntry.getValue()
									.getSubscription().toString());
						}
					}
					System.out
							.println("brokerCore >> notifyMessage >> subscriptionArray : "
									+ subscriptionList);
					this.brokerCore.setStatus("OK");

				*/} else {
					// This is the case when CSStobeMigrated subscription
					// appears for the 1st time.
					System.out.println(" <<<<<<<<<<<<<<<<<<<<<<< QueueManager --- notifyMessage --- Load Balancing starts with="+ newBrokerURIArr[0]);
					this.brokerCore.setStatus("NA");
					brokerCore.cssBitVectorCalculation(newBrokerURIArr[0]);
				}

			}
		}

	}

	public void clear() {
		queues.clear();
	}

}
