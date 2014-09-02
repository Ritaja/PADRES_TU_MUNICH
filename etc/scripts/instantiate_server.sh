#! /bin/bash


## Trim the input in the instantiate_server.sh and then compare the results to null or empty.
# Run this from console: sh instantiate_server.sh "socket://192.168.1.35:1100/BrokerA" "socket://localhost:9997/newbrokerC" "socket://192.168.1.35:1101/BrokerB","socket://192.168.1.35:1100/BrokerA"
if [ ! "$1" -o ! "$2" -o ! "$3" ]
then
	echo "format: sh instantiate.sh <overloadBrkUri> <uriLoadAcceptingBrk> <neighbors>"
	exit 0
fi


# echo arguments
overloadBrkUri=$1
uriLoadAcceptingBrk=$2
echo "LoadAccepting Broker URI=" $uriLoadAcceptingBrk
uriLoadAcceptingPort=`echo $uriLoadAcceptingBrk | cut -d"/" -f3 | cut -d":" -f2`
uriLoadAcceptingIP=`echo $uriLoadAcceptingBrk | cut -d"/" -f3 | cut -d":" -f1`
echo "LoadAccepting Broker IP" $uriLoadAcceptingIP
echo "LoadAccepting Broker port" $uriLoadAcceptingPort
neighbors=$3
searchstr="localhost"
username="topscale"
keyFile=`echo $uriLoadAcceptingBrk | cut -d"/" -f3 | cut -d":" -f1 | cut -d"." -f1`
echo "Key File =" $keyFile
echo "Current directory=" `pwd`
PADRES_HOME="$(cd $(dirname "$0")/../.. && pwd)"
export PADRES_HOME
echo "####PADRES_HOME=" $PADRES_HOME "   "
padres_dir="PADRES_TU_MUNICH"
typeOnConn=""
echo "SSH client starts.............."
echo " instantiate_server.sh >> Connecting to ...:" $uriLoadAcceptingBrk
echo "Overloaded broker is:" $overloadBrkUri
echo "Neighbors are:" $neighbors

#echo "Transformed string:" "${teststr#*$searchstr}" != "$searchstr"


if test "${uriLoadAcceptingBrk#*$searchstr}" != "$uriLoadAcceptingBrk"
then 
	echo -e "###Connecting to the new broker in the localhost####\n"			
	pwd
	#java ca.utoronto.msrg.padres.broker.brokercore.BrokerCore -uri $uriLoadAcceptingBrk -n $neighbors -ovl overloadBrkUri loadbalance
	#sh $PADRES_HOME/etc/scripts/startnewbroker.sh -uri $uriLoadAcceptingBrk -n $neighbors -ovl overloadBrkUri loadbalance
	touch ~/newBroker_"$uriLoadAcceptingIP""$uriLoadAcceptingPort".txt	
	bash $PADRES_HOME/etc/scripts/startnewbroker -uri $uriLoadAcceptingBrk -n $neighbors -ovl $overloadBrkUri loadbalance > ~/newBroker_"$uriLoadAcceptingIP""$uriLoadAcceptingPort".txt
else
	# ssh to other server yet to be done
	echo "Connecting to the new broker in a remote machine"
	touch ~/newBroker_"$uriLoadAcceptingIP""$uriLoadAcceptingPort".txt
	ssh -i ~/.ssh/$keyFile $username@$uriLoadAcceptingIP bash "~/$padres_dir/etc/scripts/startnewbroker" -uri $uriLoadAcceptingBrk -n $neighbors -ovl $overloadBrkUri loadbalance > ~/newBroker_"$uriLoadAcceptingIP""$uriLoadAcceptingPort".txt
fi


exitStatus=`echo $?`
echo "Exit status of the server process =" $exitStatus
if [ $exitStatus -eq 0 ]
then
  echo "SERVER STARTED SUCCESS!"
else
  echo "FAILED"
fi
