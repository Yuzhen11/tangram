#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/GraphMatching"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33225",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

program_params = {
    "url" : "/datasets/graph/label_skitter_8m.adj",
    #"url" : "/datasets/graph/label_skitter.adj",
    #"url" : "/datasets/graph/label_orkut.adj",
    #"url" : "/tmp/xuan/toy2.graph",
    #"url" : "/tmp/xuan/pattern.graph",
    "num_local_threads" : 20,
    "num_matcher_parts" : 400,
    "num_graph_parts" : 400,
    "num_vertices" : 1696415,
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
  "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"
  )

dump_core = False
l = Launcher(schedulerfile, progfile, hostfile,
             common_params, scheduler_params, program_params, env_params,
             dump_core)

l.Launch(sys.argv)
