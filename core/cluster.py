#! /usr/bin/env python

import os, sys, operator, time
import htcondor, subprocess, sqlite3
from optparse import OptionParser
    
def main(argv = None):
        
    if argv == None:
        argv = sys.argv[1:]
                    
    home = os.getcwd()
                            
    usage = "usage: %prog [options]\n Get data from cluster and store it in a database"
    
    parser = OptionParser(usage)
    parser.add_option("--generalDB", default="general.db", help="Name of general database [default: %default]")
    parser.add_option("--userDB", default="user.db", help="Name of user database [default: %default]")
    parser.add_option("--nodeDB", default="node.db", help="Name of node database [default: %default]")
    parser.add_option("--recreate", action="store_true", help="Recreate database [default: %default]")
    parser.add_option("--max", type=int, default=100000, help="Maximum number of rows [default: %default]")
    parser.add_option("--enc", type=str, default="utf-8", help="Encoding [default: %default]")
    parser.add_option("--output", type=str, default="jobs", help="Output directory [default: %default]")
    
    (options, args) = parser.parse_args(sys.argv[1:])

    return options

def writeDB(con, cur, name, len):
    
    nentries = cur.execute('SELECT COUNT(*) FROM '+name).fetchone()[0]

    query = 'DELETE FROM '+name+' where ts IN (SELECT ts from '+name+' order by ts ASC limit (?))'
    if nentries > options.max:
        cur.execute(''+query+'', [len])

    con.commit()
    con.close()    

if __name__ == '__main__':
                    
    options = main()

    schedds = []
    for schedd_ad in htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd):
        schedds.append(schedd_ad)

    proc = subprocess.Popen(["condor_userprio", "-usage", "-allusers"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outUserPrio, errUserPrio = proc.communicate()        

    totJobs, idleJobs, runJobs, heldJobs = {}, {}, {}, {}
    for sch in schedds:
        schedd = htcondor.Schedd(sch)
        sName = sch['Name']        
        totJobs[sName] = schedd.xquery()
        idleJobs[sName] = schedd.xquery('JobStatus == 1')
        runJobs[sName] = schedd.xquery('JobStatus == 2')
        heldJobs[sName] = schedd.xquery('JobStatus == 5')
    
    userUsage, userJobs = {}, {}
    
    for line in outUserPrio.splitlines():
        l = line.decode(options.enc)
        if 'iihe.ac.be' in l:
            ud = l.split()
            user = ud[0].split('@')[0]
            if user == '': continue
            usage = float(ud[2])

            if user != '':
                userUsage[user] = usage
                
    for sch in schedds:            
        schedd = htcondor.Schedd(sch)
        for k in userUsage.keys():
            query = schedd.query('Owner == \"'+k+'\"', projection=["ProcID"])
            if k not in userJobs: userJobs[k] = 0
            userJobs[k] += len(query)

    data = []
    ts = time.time()
    tsr = time.ctime(ts)
    for k in userUsage.keys():
        data.append((k, ts, tsr, userUsage[k], userJobs[k]))
    ntotJobs = [len(list(v)) for v in totJobs.values()]
    nrunJobs = [len(list(v)) for v in runJobs.values()]
    nidleJobs = [len(list(v)) for v in idleJobs.values()]
    nheldJobs = [len(list(v)) for v in heldJobs.values()]
    data_general = [(ts, tsr, int(sum(ntotJobs)), int(sum(nrunJobs)), int(sum(nidleJobs)), int(sum(nheldJobs)))]
            
    # user DB
    if options.recreate and os.path.isfile(options.userDB): os.system('rm -f '+options.userDB)
    con = sqlite3.connect(options.userDB)
    cur = con.cursor()
    
    tab = cur.execute('''SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'info\'''')

    if tab.fetchone() is None:
        cur.execute('''CREATE TABLE info (user text, ts integer, time text, usage real, jobs real)''')
        con.commit()
        
    cur.executemany("INSERT INTO info VALUES (?, ?, ?, ?, ?)", data)
    writeDB(con, cur, 'info', len(data))
    
    # general DB
    if options.recreate and os.path.isfile(options.generalDB): os.system('rm -f '+options.generalDB)
    con = sqlite3.connect(options.generalDB)
    cur = con.cursor()
    
    tab = cur.execute('''SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'info\'''')

    if tab.fetchone() is None:
        cur.execute('''CREATE TABLE info (ts integer, time text, totjobs integer, runjobs integer, idlejobs integer, heldjobs integer)''')
        con.commit()
        
    cur.executemany("INSERT INTO info VALUES (?, ?, ?, ?, ?, ?)", data_general)
    writeDB(con, cur, 'info', len(data_general))
            
    # node DB
    if options.recreate and os.path.isfile(options.nodeDB): os.system('rm -f '+options.nodeDB)
    con = sqlite3.connect(options.nodeDB)
    cur = con.cursor()

    tab = cur.execute('''SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'node\'''')
    
    if tab.fetchone() is None:
        cur.execute('''CREATE TABLE node (name text, ts integer, time text, cputot integer, cpubusy integer, memtot integer, membusy integer, machineload real, slotload real, cputime real, walltime real, cpueff real)''')
        con.commit()

    proc = subprocess.Popen(["condor_status -af Machine DetectedCpus DetectedMemory TotalCondorLoadAvg CondorLoadAvg"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    machineData = list(dict.fromkeys(out.splitlines()))
    machines = {}
    for i, d in enumerate(machineData):
        machineData[i] = d.decode(options.enc).split()
        machines[machineData[i][0]] = {'cpu': machineData[i][1], 'mem': machineData[i][2], 'machineload': machineData[i][3], 'slotload': machineData[i][4],}

    proc = subprocess.Popen(["condor_status -af Name Cpus Memory Activity RemoteUser"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    slotData = out.splitlines()
    slots = {}
    for i, s in enumerate(slotData):
        slotData[i] = s.decode(options.enc).split()
        name = slotData[i][0]
        slotName = name.split('@')[0]
        if slotName in ['slot1']: continue
        machineName = name.split('@')[-1]
        if machineName not in slots: slots[machineName] = []
        slots[machineName].append({'name': slotName, 'fullname': name, 'cpu': slotData[i][1], 'mem': slotData[i][2], 'activity': slotData[i][3], 'user': slotData[i][4]})

    for m in slots.keys():
        nCoreTot = machines[m]['cpu']
        nMemTot = machines[m]['mem']
        nCoreBusy = 0
        nMemBusy = 0
        for s in slots[m]:
            nCoreBusy += int(s['cpu'])
            nMemBusy += int(s['mem'])
        machines[m].update({'cputot': nCoreTot, 'cpubusy': nCoreBusy, 'memtot': nMemTot, 'membusy': nMemBusy})

    proccpu = subprocess.Popen(["condor_q -g -all -run -af RemoteUserCpu JobStartDate RemoteHost ClusterId Owner RequestCpus -constraint 'RemoteHost != \"undefined\"'"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    outcpu, errcpu = proccpu.communicate()
    cpuData = outcpu.splitlines()
    
    tnow = float(time.time())

    cpuInfo = {}
    for i, s in enumerate(cpuData):
        
        cpuData[i] = s.decode(options.enc).split()

        name = cpuData[i][2]
        machineName = name.split('@')[-1]
        found = False
        for sl in slots[machineName]:
            if sl['fullname'] == name:
                found = True
                break
        if not found: continue
        
        cpuUsage = float(cpuData[i][0])/float(cpuData[i][5]) # per cpu core
        jobStart = float(cpuData[i][1])
        jobWall = tnow - jobStart
        
        if machineName not in cpuInfo: cpuInfo[machineName] = {'cpu': [], 'wall': []}
        cpuInfo[machineName]['cpu'].append(cpuUsage)
        cpuInfo[machineName]['wall'].append(jobWall)
    
    for m in cpuInfo.keys():
        cpuData = cpuInfo[m]['cpu']
        wallData = cpuInfo[m]['wall']
        cpuTot = sum(cpuData)
        wallTot = sum(wallData)
        cpuEff = cpuTot/wallTot
        machines[m].update({'cputime': cpuTot, 'walltime': wallTot, 'cpueff': cpuEff})

    data = []
    ts = time.time()
    tsr = time.ctime(ts)
    for m in machines.keys():
        if 'cputot' in machines[m] and 'cputime' in machines[m]:
            data.append((m, ts, tsr, machines[m]['cputot'], machines[m]['cpubusy'], machines[m]['memtot'], machines[m]['membusy'], machines[m]['machineload'], machines[m]['slotload'], machines[m]['cputime'], machines[m]['walltime'], machines[m]['cpueff']))
        
    cur.executemany("INSERT INTO node VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
    writeDB(con, cur, 'node', len(data))
