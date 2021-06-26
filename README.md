# HTCondor-LiteView

![Python](https://img.shields.io/badge/Python-3-red.svg)
![Numpy](https://img.shields.io/badge/Numpy->=1.16.2-blue.svg)
![Pandas](https://img.shields.io/badge/Pandas->=0.23.1-blue.svg)
![Plotly](https://img.shields.io/badge/Plotly-4.14.3-red.svg)

A lightweight web-based framework to monitor the performance of the
[HTCondor](https://github.com/htcondor/htcondor) cluster that can be installed in any web-accessible area
without administration rights on the deployment machine.

Installation:

```
git clone https://github.com/kskovpen/HTCondor-LiteView
cd HTCondor-LiteView
```

Specify the output web and database installation directories in the run.sh
and cronjob files. In order to start a continuous monitoring of the cluster, submit a cron job:

```
crontab cronjob
```
