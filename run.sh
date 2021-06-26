#!/bin/bash

echo "Date: `date`"

export PATH=/user/kskovpen/dist/python3/bin:$PATH
export LD_LIBRARY_PATH=/user/kskovpen/dist/python3/lib:LD_LIBRARY_PATH

monDir="/user/kskovpen/public_html/cluster_dev"

dbdir="/user/kskovpen/webtools/dev/db/"
[ ! -d ${ddir} ] && mkdir -p ${ddir}

generalDB="${dbdir}/general.db"
userDB="${dbdir}/user.db"
nodeDB="${dbdir}/node.db"

python3 /user/kskovpen/webtools/dev/core/cluster.py --generalDB=${generalDB} --userDB=${userDB} --nodeDB=${nodeDB}

python3 /user/kskovpen/webtools/dev/core/plot.py --generalDB=${generalDB} --userDB=${userDB} --nodeDB=${nodeDB} --output=${monDir}
