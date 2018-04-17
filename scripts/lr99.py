#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/DenseLRExample"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33224",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

# for webspam
webspam_params = {
    "url" : "/ml/webspam",
    "num_data" : 350000,
    "num_params" : 16609143,
    "num_param_per_part" : 16609,
}

# for a9
a9_params = {
    "url" : "/jasper/a9",
    "num_data" : 32561,
    "num_params" : 123,
    "num_param_per_part" : 10,
}

# for avazu
avazu_params = {
    "url" : "/jasper/avazu-app",
    "num_data" : 40428967,
    "num_params" : 1000000,
    "num_param_per_part" : 1000000,
}

program_params = {
    "num_local_threads" : 20,
    "num_data_parts" : 400,
    "batch_size" : 800,
    "alpha" : 0.001,
    "num_iter" : 5,
    "staleness" : 0,
    "is_sparse" : False,
    "is_sgd" : False,
    # to make FT work, do not use kShuffleCombine,
    # to make it fast, use kShuffleCombine 
    "combine_type" : "kShuffleCombine",
    "max_lines_per_part" : -1,
}

# choose one of them
program_params.update(webspam_params)
# program_params.update(a9_params)
# program_params.update(avazu_params)

if program_params["is_sparse"]:
  progfile = "release/SparseLRExample"

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
