#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/5nodes"
progfile = "release/Nomad"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj10",
    "scheduler_port" : "33214",
    "hdfs_namenode" : "proj10",
    "hdfs_port" : 9000,
}

toy_data = {
    "url" : "/yuzhen/als_toy.txt",
    "kNumUser" : 3,
    "kNumItem" : 3,
}
netflix_data = {
    "url" : "/datasets/ml/netflix",
    "kNumUser" : 480189,
    "kNumItem" : 17770,
}

dataset = netflix_data 
# dataset = toy_data 

program_params = {
    "num_local_threads": 20,
    "url" : dataset["url"],
    "kNumItem" : dataset["kNumItem"],
    "kNumUser" : dataset["kNumUser"],
    "iter": 100, 
    "eta" : 0.01,
    "lambda" : 0.05,
    "kNumPartition" : 20,
    "staleness": 0,
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
