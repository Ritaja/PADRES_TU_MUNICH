#! /bin/bash

# echo arguments
overloadBrkUri=$1
uriLoadAcceptingBrk=$2
neighbors=$3
typeOnConn=""
echo "SSH client starts.............."
echo " instantiate_server.sh >> Connecting to .... " $uriLoadAcceptingBrk
echo "Overloaded broker is " $overloadBrkUri
echo "Neighbors are " $neighbors

if 
if [ "$hostname" = "localhost" ] || [ "$hostname" = "127.0.0.1" ]; then
	echo " instantiate.sh >> found local host "
	echo "current path "
	pwd
	#cd ../../
	nohup java ca.utoronto.msrg.padres.broker.brokercore.BrokerCore -n $neighbors -ovl $overloadBrkUri loadbalancing &
	echo " instantiate.sh >> exit status " $!
	echo " instantiate.sh >> process started has PID as" $PID
else
	echo " instantiate.sh >> others port =" $port
	ssh $username@$hostname java -jar ../../ms3-server.jar $port INFO &
	echo " instantiate.sh >> exit status " $!
fi 


echo "\n instantiate.sh >> SSH script ends for  .... " $hostname ":" $port
