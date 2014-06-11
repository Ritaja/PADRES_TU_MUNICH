#! /bin/bash

# echo arguments
hostname=$1
username="hazra"
port=$2
#metadata=$3

directory=
echo " instantiate.sh >> Connecting to .... " $hostname ":" $port
if [ "$hostname" = "localhost" ] || [ "$hostname" = "127.0.0.1" ]; then
	echo " instantiate.sh >> found local host "
	echo "current path "
	pwd
	#nohup java -jar ms3-server.jar $port INFO &
	echo " instantiate.sh >> exit status " $!
	cat tmp.txt
	echo " instantiate.sh >> process started has PID as" $PID
else
	echo " instantiate.sh >> others port =" $port
	ssh $username@$hostname java -jar ../../ms3-server.jar $port INFO &
	echo " instantiate.sh >> exit status " $!
fi 


echo "\n instantiate.sh >> SSH script ends for  .... " $hostname ":" $port
