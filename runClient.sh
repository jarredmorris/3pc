#!/bin/bash

# We run the client program on the supplied port.

# Note that if you choose to change the client port you must also change it in
# runServers.sh

clientPort=9050;
java -cp TPC.jar dcs.os.Client localhost:$clientPort;

exit 0;
