#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/5nodes"
progfile = "debug/LRExample"
schedulerfile = "debug/SchedulerMain"

common_params = {
    "scheduler" : "proj10",
    "scheduler_port" : "33224",
    "hdfs_namenode" : "proj10",
    "hdfs_port" : 9000,
}

program_params = {
    "url" : "/datasets/classification/a9",
    "num_local_threads" : 20,
    "num_params" : 123,
    "num_data" : 32561,
    "alpha" : 0.005,
    "num_iter" : 50,
    "staleness" : 2,
}

scheduler_params = {
    "num_worker" : 5,
    "dag_runner_type" : "sequential",
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
