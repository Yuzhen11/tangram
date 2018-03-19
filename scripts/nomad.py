#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/5nodes"
progfile = "debug/Nomad"
schedulerfile = "debug/SchedulerMain"

common_params = {
    "scheduler" : "proj10",
    "scheduler_port" : "33214",
    "hdfs_namenode" : "proj10",
    "hdfs_port" : 9000,
}

program_params = {
    "url" : "/yuzhen/als_toy.txt",
    "iter": 5, 
    "kNumItem" : 3,
    "kNumUser" : 3,
    "eta" : 0.1,
    "lambda" : 0.1,
    "kNumPartition" : 2,
}

scheduler_params = {
    "num_worker" : 5,
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  # this is to enable hdfs short-circuit read (disable the warning info)
  # change this path accordingly when we use other cluster
  # the current setting is for proj5-10
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

dump_core = False 
l = Launcher(schedulerfile, progfile, hostfile,
             common_params, scheduler_params, program_params, env_params,
             dump_core)

l.Launch(sys.argv)
