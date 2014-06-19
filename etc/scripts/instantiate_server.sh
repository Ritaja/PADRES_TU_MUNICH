#! /bin/bash


## Trim the input in the instantiate_server.sh and then compare the results to null or empty.

if [ ! "$1" -o ! "$2" -o ! "$3" ]
then
	echo "format: sh instantiate.sh <overloadBrkUri> <uriLoadAcceptingBrk> <neighbors>"
	exit 0
fi


# echo arguments
overloadBrkUri=$1
uriLoadAcceptingBrk=$2
neighbors=$3
searchstr="localhost"

 
typeOnConn=""
echo "SSH client starts.............."
echo " instantiate_server.sh >> Connecting to ...:" $uriLoadAcceptingBrk
echo "Overloaded broker is:" $overloadBrkUri
echo "Neighbors are:" $neighbors

#echo "Transformed string:" "${teststr#*$searchstr}" != "$searchstr"


if test "${uriLoadAcceptingBrk#*$searchstr}" != "$uriLoadAcceptingBrk"
then 
	echo "Connecting to the new broker in the localhost"
	pwd
	cd ../../
	java ca.utoronto.msrg.padres.broker.brokercore.BrokerCore -uri $uriLoadAcceptingBrk -n $neighbors -ovl overloadBrkUri loadbalance 
	
else
	echo "Connecting to the new broker in a remote machine"
	ssh $username@$hostname java -jar ../../ms3-server.jar $port INFO 
fi
