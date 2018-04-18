#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/KmeansExample"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33424",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

# for SVHN
svhn_params = {
    "url" : "/jasper/SVHN",
    "num_data" : 73257,
    "num_dims" : 3072,
    "num_param_per_part" : 3072,
    "K" :10,
}

# for webspam
mnist8m_params = {
    "url" : "/jasper/mnist8m",
    "num_data" : 8100000,
    "num_dims" : 784,
    "num_param_per_part" : 784*11,
    "K" : 10,
}

# for a9
a9_params = {
    "url" : "/jasper/a9",
    "num_data" : 32561,
    "num_dims" : 123,
    "num_param_per_part" : 123,
    "K" : 2,
}

# for avazu
avazu_params = {
    "url" : "/jasper/avazu-app",
    "num_data" : 40428967,
    "num_dims" : 1000000,
    "num_param_per_part" : 1000000,
    "K" : 2,
}

program_params = {
    "num_local_threads" : 20,
    "num_data_parts" : 1000,
    "batch_size" : 1000,
    "alpha" : 0.1,
    "num_iter" : 10,
    "staleness" : 0,
    "is_sgd" : False,
    # to make FT work, do not use kShuffleCombine,
    # to make it fast, use kShuffleCombine
    "combine_type" : "kDirectCombine",
    "max_lines_per_part" : -1,
    "replicate_factor" : 10,
}

# choose one of them
program_params.update(mnist8m_params)
# program_params.update(a9_params)
# program_params.update(svhn_params)

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
