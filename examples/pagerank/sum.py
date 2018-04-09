#!/usr/bin/env python

import sys
import os.path
from os.path import dirname, realpath 

proj_dir = dirname(dirname(dirname(realpath(__file__))))
print proj_dir
sys.path.append(proj_dir+"/scripts/")

from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/Sum"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33227",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

program_params = {
    "url": "/tmp/tmp/yz/tmp/0409/50-10/pr",
}

scheduler_params = {
    "dag_runner_type" : "sequential",
}

env_params = (
  "GLOG_logtostderr=true "
  "GLOG_v=-1 "
  "GLOG_minloglevel=0 "
  # this is to enable hdfs short-circuit read (disable the warning info)
  # change this path accordingly when we use other cluster
  # the current setting is for proj5-10
  # "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  "LIBHDFS3_CONF=/data/opt/hadoop-2.6.0/etc/hadoop/hdfs-site.xml"
  )

dump_core = False
l = Launcher(schedulerfile, progfile, hostfile,
             common_params, scheduler_params, program_params, env_params,
             dump_core)

l.Launch(sys.argv)

# for i in xrange(10):
#     program_params["url"] = "/tmp/tmp/yz/tmp/0408/pr3/"+str(i)
#     l = Launcher(schedulerfile, progfile, hostfile,
#                  common_params, scheduler_params, program_params, env_params,
#                  dump_core)
#
#     l.Launch(sys.argv)
#
