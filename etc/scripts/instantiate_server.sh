#! /bin/bash

# echo arguments
overloadBrkUri=$1
uriLoadAcceptingBrk=$2
neighbors=$3
searchstr="localhost"

typeOnConn=""
echo "SSH client starts.............."
echo " instantiate_server.sh >> Connecting to .... " $uriLoadAcceptingBrk
echo "Overloaded broker is " $overloadBrkUri
echo "Neighbors are " $neighbors

if if test "${teststr#*$searchstr}" != "$teststr"
then 
	echo "Connecting to the new broker in the localhost"
	pwd
	java ca.utoronto.msrg.padres.broker.brokercore BrokerCore.java 
	
else
	echo "Connecting to the new broker in a remote machine"
	ssh $username@$hostname java -jar ../../ms3-server.jar $port INFO 
fi