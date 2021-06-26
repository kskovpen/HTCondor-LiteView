#! /usr/bin/env python

import os, sys, operator, time, itertools
import sqlite3
import pandas as pd
import statistics
pd.options.plotting.backend = "plotly"
from optparse import OptionParser

def main(argv = None):
        
    if argv == None:
        argv = sys.argv[1:]
                    
    home = os.getcwd()
                            
    usage = "usage: %prog [options]\n Plot data from database"
    
    parser = OptionParser(usage)
    parser.add_option("--generalDB", default="general.db", help="Name of general database [default: %default]")
    parser.add_option("--userDB", default="user.db", help="Name of user database [default: %default]")
    parser.add_option("--nodeDB", default="node.db", help="Name of node database [default: %default]")
    parser.add_option("--limit", type=int, default=1000000, help="Limit on the number of readout rows [default: %default]")
    parser.add_option("--usermax", type=int, default=100, help="Maximum number of top users to plot [default: %default]")
    parser.add_option("--output", default="/user/kskovpen/public_html/cluster_dev", help="Output location [default: %default]")
    parser.add_option("--hostname", default="wn.iihe.ac.be", help="Host location name [default: %default]")
    
    (options, args) = parser.parse_args(sys.argv[1:])

    return options

if __name__ == '__main__':
                    
    options = main()
    
    if not os.path.isdir(options.output): os.system('mkdir '+options.output)
    if not os.path.isdir(options.output+'/plots'): os.system('mkdir '+options.output+'/plots')
    if not os.path.isfile(options.output+'/mon.html'): os.system('cp mon.html '+options.output+'/')

    # user DB
    con = sqlite3.connect(options.userDB)
    cur = con.cursor()

    records = cur.execute('SELECT * FROM info ORDER BY ts ASC LIMIT (?)', [options.limit])

    # group by timestamp
    data = []
    for ts, v in itertools.groupby(records, key=lambda r: r[1]):
        li = list(v)
        li = [tuple([l[0].replace('b\'', '')] + list(l[1:])) for l in li]
        data.append(li)

    con.commit()
    con.close()
    
    # users with the largest usage
    usermaxusage = []
    for d in data:
        d.sort(key=lambda r: r[3], reverse=True)
        topusage = d[0:options.usermax]
        usermaxusage.append([k[0] for k in topusage])
    namesmaxusage = []
    for u in usermaxusage:
        for uu in u:
            if uu not in namesmaxusage: namesmaxusage.append(uu)

    # users with the largest number of jobs
    usermaxjobs = []
    for d in data:
        d.sort(key=lambda r: r[4], reverse=True)
        topjobs = d[0:options.usermax]
        usermaxjobs.append([k[0] for k in topjobs])
    namesmaxjobs = []
    for u in usermaxjobs:
        for uu in u:
            if uu not in namesmaxjobs: namesmaxjobs.append(uu)
    
    # usage
    tss = []    
    usages = {}
    for ientry, d in enumerate(data):
        for iu, u in enumerate(d):
            name, ts, usage = u[0], u[1], u[3]
            if name not in namesmaxusage: continue
            if iu == 0: tss.append(ts)
            if name not in usages: usages[name] = []
            usages[name].append(usage)

    dusage = {'ts': tss}
    for name in namesmaxusage:
        dusage.update({name:usages[name]})
        
    df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in dusage.items() ]))
    df = df.sort_values(['ts'])

    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    
    fig = df.plot(x='ts', y=namesmaxusage)
    fig.update_layout(xaxis_title='Time', yaxis_title='Usage', legend_title='Users:')
    fig.update_yaxes(type='log')
    fig.write_html(options.output+"/plots/userusage.html")

    # jobs
    jobss = {}
    for ientry, d in enumerate(data):
        for iu, u in enumerate(d):
            name, jobs = u[0], u[4]
            if name not in namesmaxjobs: continue
            if name not in jobss: jobss[name] = []
            jobss[name].append(jobs)

    djobs = {'ts': tss}
    for name in namesmaxjobs:
        djobs.update({name:jobss[name]})
        
    df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in djobs.items() ]))
    df = df.sort_values(['ts'])

    df['ts'] = pd.to_datetime(df['ts'], unit='s')

    fig = df.plot(x='ts', y=namesmaxjobs)
    fig.update_layout(xaxis_title='Time', yaxis_title='Jobs', legend_title='Users:')
    fig.update_yaxes(type='log')
    fig.write_html(options.output+"/plots/userjobs.html")
    
    # general DB
    con_general = sqlite3.connect(options.generalDB)
    cur_general = con_general.cursor()

    data = list(cur_general.execute('SELECT * FROM info ORDER BY ts ASC LIMIT (?)', [options.limit]))
    
    con_general.commit()

    # total number of jobs
    totjobs, runjobs, idlejobs, heldjobs = [], [], [], []
    for d in data:
        totjobs.append(d[2])
        runjobs.append(d[3])
        idlejobs.append(d[4])
        heldjobs.append(d[5])

    tss = []
    for d in data:
        tss.append(d[0])
        
    df = pd.DataFrame({'ts': tss, 'total': totjobs, 'running': runjobs, 'idle': idlejobs, 'held': heldjobs})
    df = df.sort_values(['ts'])

    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    
    fig = df.plot(x='ts', y=['total', 'running', 'idle', 'held'])
    fig.for_each_trace(lambda trace: trace.update(visible = "legendonly") if trace.name not in ["running", "total"] else (),)
    fig.for_each_trace(lambda trace: trace.update(line = dict(width=4)))
    fig.update_layout(xaxis_title='Time', yaxis_title='Number of jobs', legend_title='State:')
    fig.write_html(options.output+"/plots/generaljobs.html")
    
    con_general.close()

    # node DB
    con_node = sqlite3.connect(options.nodeDB)
    cur_node = con_node.cursor()

    data = list(cur_node.execute('SELECT * FROM node ORDER BY ts ASC LIMIT (?)', [options.limit]))
    
    con_node.commit()

    tss, machine, cputot, cpubusy, memtot, membusy, machineload, slotload, cputime, walltime, cpueff = ([] for _ in range(11))
    for d in data:
        tss.append(d[1])
        machine.append(d[0].replace('.'+options.hostname, ''))
        cputot.append(d[3])
        cpubusy.append(d[4])
        memtot.append(d[5])
        membusy.append(d[6])
        machineload.append(d[7])
        slotload.append(d[8])
        cputime.append(d[9])
        walltime.append(d[10])
        cpueff.append(d[11])
        
    df = pd.DataFrame({'ts': tss, 'machine': machine, 'cputot': cputot, 'cpubusy': cpubusy, 'memtot': memtot, 'membusy': membusy, 'machineload': machineload, 'slotload': slotload, 'cputime': cputime, 'walltime': walltime, 'cpueff': cpueff})
    df = df.sort_values(['ts'])

    df['ts'] = pd.to_datetime(df['ts'], unit='s')
    
    dfts = df.iloc[[-1]]['ts'].iloc[0]
    dflatest = df[df['ts'] == dfts]
    dflatest = dflatest.sort_values(['machine'])
    machineList = [k for k in dflatest['machine']]
    machineNameMax = max(machineList, key=len)

    # group by timestamp
    datatmp = []
    for ts, v in itertools.groupby(data, key=lambda r: r[1]):
        li = list(v)
        li = [tuple([l[0].replace('b\'', '')] + list(l[1:])) for l in li]
        datatmp.append(li)
    
#    fig = df.plot(x='ts', y=['total', 'running', 'idle', 'held'])
#    fig.for_each_trace(lambda trace: trace.update(visible = "legendonly") if trace.name != "running" else (),)
#    fig.for_each_trace(lambda trace: trace.update(line = dict(width=4)))
#    fig.update_layout(xaxis_title='Time', yaxis_title='Number of jobs', legend_title='State:')
#    fig.write_html(options.output+"/plots/generaljobs.html")
    
    con_node.close()

    # CPU load for the last entry
#    machineload = [k for k in dflatest['machineload']]
#    cputot = [k for k in dflatest['cputot']]
#    loadrel = [k/cputot[ik] for ik, k in enumerate(machineload)]
#    loadrelTruncated = [(k if k <= 1. else 0.9999) for k in loadrel]
#    loadcol = []
#    for l in loadrel:
#        if l > 1.: loadcol.append("#F32016")
#        elif l > 0.5: loadcol.append("#FFFD96")
#        else: loadcol.append("#92F162")

    nLast = 10
    dtfs = list(df['ts'].drop_duplicates().tail(nLast))
    dflatest = df.query('ts in @dtfs')
    machineload, cputot, loadrel, loadrelTruncated = [], [], [], []
    for m in machineList:
        dfinfo = dflatest[dflatest['machine'] == m]
        machineLoadData = list(dfinfo['machineload'])
        cpuTotData = list(dfinfo['cputot'])
        machineload.append(statistics.mean(machineLoadData))
        cputot.append(statistics.mean(cpuTotData))
        lr = [k/cpuTotData[ik] for ik, k in enumerate(machineLoadData)]
        lrt = [(k if k <= 1. else 0.9999) for k in lr]
        loadrel.append(statistics.mean(lr))
        loadrelTruncated.append(statistics.mean(lrt))

    loadcol = []
    for l in loadrel:
        if l > 1.: loadcol.append("#F32016")
        elif l > 0.5: loadcol.append("#FFFD96")
        else: loadcol.append("#92F162")
        
    with open(options.output+"/plots/cpuload.html", "w") as fnode:
        fnode.write('<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>\n')
        fnode.write('<span style="display:none;" id="string_span">'+machineNameMax+'</span>\n')
        fnode.write('<script>\n')
        fnode.write('function createTable() {\n')
        fnode.write('var nodeNameF = $("#string_span").width()*1.05;\n')
        fnode.write('var tabw = window.innerWidth*0.93;\n')
        nLineMax = 7
        fnode.write('var namef = nodeNameF/(tabw/'+str(nLineMax)+');\n')
        nc = 0
        for im, m in enumerate(machineList):
            if (im)%nLineMax == 0:
                nc = 0
                fnode.write('document.getElementById("nodetable'+str(im)+'").setAttribute("style","table-layout:fixed;width:"+tabw+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'").setAttribute("style","width:"+tabw/'+str(nLineMax)+'+"px;");\n')
            fnode.write('document.getElementById("tdname'+str(im)+'").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*namef+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'busy").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*(1-namef)*'+str(loadrelTruncated[im])+'+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'free").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*(1-namef)*(1-'+str(loadrelTruncated[im])+')+"px;");\n')
            nc += 1
            if im == len(machineList)-1 or (im)%nLineMax != 0:
                fnode.write('document.getElementById("nodetable'+str(im-nc+1)+'").setAttribute("style","table-layout:fixed;width:"+tabw*'+str(nc/nLineMax)+'+"px;");\n')
        fnode.write('}</script>\n')
        for im, m in enumerate(machineList):
            if (im)%nLineMax == 0:
                if im > 0: fnode.write('</tr></table>\n')
                fnode.write('<table id="nodetable'+str(im)+'"><tr>\n')
            fnode.write('<span id="td'+str(im)+'"><td id="tdname'+str(im)+'" bgcolor="#FFF">'+m+'</td><td id="td'+str(im)+'busy" bgcolor="'+loadcol[im]+'" align="right">'+"{:.1f}".format(loadrel[im]*100.)+'%</td><td id="td'+str(im)+'free" bgcolor="#FFF"></td></span>\n')
        fnode.write('</tr></table><script>createTable();</script>\n')
        fnode.close()

    # CPU efficiency
    nLast = 10
    dtfs = list(df['ts'].drop_duplicates().tail(nLast))
    dflatest = df.query('ts in @dtfs')
    cpueff = []
    for m in machineList:
        dfinfo = dflatest[dflatest['machine'] == m]
        cpueff.append(statistics.mean(list(dfinfo['cpueff'])))

    cpucol = []
    for l in cpueff:
        if l > 0.90: cpucol.append("#92F162")
        elif l > 0.50: cpucol.append("#FFFD96")
        else: cpucol.append("#F32016")

    with open(options.output+"/plots/cpueff.html", "w") as fnode:
        fnode.write('<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>\n')
        fnode.write('<span style="display:none;" id="string_span">'+machineNameMax+'</span>\n')
        fnode.write('<script>\n')
        fnode.write('function createTable() {\n')
        fnode.write('var nodeNameF = $("#string_span").width()*1.05;\n')
        fnode.write('var tabw = window.innerWidth*0.93;\n')
        nLineMax = 7
        fnode.write('var namef = nodeNameF/(tabw/'+str(nLineMax)+');\n')
        nc = 0
        for im, m in enumerate(machineList):
            if (im)%nLineMax == 0:
                nc = 0
                fnode.write('document.getElementById("nodetable'+str(im)+'").setAttribute("style","table-layout:fixed;width:"+tabw+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'").setAttribute("style","width:"+tabw/'+str(nLineMax)+'+"px;");\n')
            fnode.write('document.getElementById("tdname'+str(im)+'").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*namef+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'busy").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*(1-namef)*'+str(cpueff[im])+'+"px;");\n')
            fnode.write('document.getElementById("td'+str(im)+'free").setAttribute("style","width:"+tabw/'+str(nLineMax)+'*(1-namef)*(1-'+str(cpueff[im])+')+"px;");\n')
            nc += 1
            if im == len(machineList)-1 or (im)%nLineMax != 0:
                fnode.write('document.getElementById("nodetable'+str(im-nc+1)+'").setAttribute("style","table-layout:fixed;width:"+tabw*'+str(nc/nLineMax)+'+"px;");\n')
        fnode.write('}</script>\n')
        for im, m in enumerate(machineList):
            if (im)%nLineMax == 0:
                if im > 0: fnode.write('</tr></table>\n')
                fnode.write('<table id="nodetable'+str(im)+'"><tr>\n')
            fnode.write('<span id="td'+str(im)+'"><td id="tdname'+str(im)+'" bgcolor="#FFF">'+m+'</td><td id="td'+str(im)+'busy" bgcolor="'+cpucol[im]+'" align="right">'+"{:.1f}".format(cpueff[im]*100.)+'%</td><td id="td'+str(im)+'free" bgcolor="#FFF"></td></span>\n')
        fnode.write('</tr></table><script>createTable();</script>\n')
        fnode.close()

    tss = []    
    cpueff = {}
    for ientry, d in enumerate(datatmp):
        for iu, u in enumerate(d):
            name, ts, eff = u[0], u[1], u[11]
            name = name.replace('.'+options.hostname, '')
            if name not in machineList: continue
            if iu == 0: tss.append(ts)
            if name not in cpueff: cpueff[name] = []
            cpueff[name].append(eff)

    deff = {'ts': tss}
    for name in machineList:
        deff.update({name:cpueff[name]})
        
    df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in deff.items() ]))
    df = df.sort_values(['ts'])

    df['ts'] = pd.to_datetime(df['ts'], unit='s')

    fig = df.plot(x='ts', y=machineList)
    fig.update_layout(xaxis_title='Time', yaxis_title='CPU efficiency', legend_title='Nodes:')
    fig.write_html(options.output+"/plots/cpuefftime.html")
