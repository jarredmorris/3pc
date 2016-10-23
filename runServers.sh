#!/bin/bash
# runServers.sh takes in one argument - the number of servers - and runs this
# many instances of Node.java with the relevant settings.

# If we have anything other than one argument supplied to the script then it has
# been misused so we exit with a non-zero exit status (indicating failure)
if [[ $# -ne 1 ]]
then
	printf "...Usage of runServers.sh:\n";
	printf "\t ./runServers.sh <number of servers>\n";
	printf "\t\t Examples: ./runServers.sh 15\n";
	exit 1;
fi

# n is set to be the one argument supplied to the script on the command line
n=$1;

# We create n servers: 1 coordinator node and (n-1) cohorts.
numCohorts=`expr $n - 1`;

# Coordinator runs on coordPort, client on clientPort
# Note that if you want to change the clientPort you must also change it in
# the runClient.sh file.
coordPort=9030;
clientPort=9050;

java -cp .:TPC.jar Node coordinator $coordPort $clientPort $numCohorts db1.txt &

for ((i=2; i<=$n; i++)); do
	java -cp .:TPC.jar Node cohort localhost:$coordPort db$i.txt &
done

exit 0;
