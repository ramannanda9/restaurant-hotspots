#!/bin/bash

DATAPATH=$1
CLUSTERS=$2
ITERS=$3
SAVEPATH=$4

mvn exec:java -Dexec:MainClass="analysis.MainClass" -Dexec.args="${DATAPATH} ${CLUSTERS} ${ITERS} ${SAVEPATH}"

