#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/Nomad"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "38214",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

toy_data = {
    "url" : "/ml/als_toy.txt",
    "kNumUser" : 3,
    "kNumItem" : 3,
    # "eta" : 0.01,

    "alpha" : 0.1,
    "beta" : 0,
    "lambda" : 0.05,
    "num_line_per_part": -1,
    "kNumPartition" : 2,
    "iter": 100, 
    "staleness": 0,
}

netflix_data = {
    "url" : "/ml/netflix",
    "kNumUser" : 480189,
    "kNumItem" : 17770,

    "alpha" : 0.001,
    "beta" : 0.05,
    "lambda" : 0.05,
    "num_line_per_part": -1,
    "kNumPartition" : 200,
    "iter": 100, 
    "staleness": 100,
}

yahoo_data = {
    "url" : "/ml/yahoomusic",
    "kNumUser" : 1823179,
    "kNumItem" : 136736,

    # "alpha" : 0.000075,
    "alpha" : 0.0003,
    "beta" : 0.01,
    "lambda" : 1,
    "num_line_per_part": -1,
    "kNumPartition" : 200,
    "iter": 400, 
    "staleness": 0,
}

dataset_param = yahoo_data 
# dataset_param = netflix_data 
# dataset_param = toy_data 

program_params = {
    "num_local_threads": 20,
    "backoff_time" : 10,
    "max_sample_item_size_each_round": 500,
    "max_retry" : 0,  # may need to set to 0 for bsp
}
program_params.update(dataset_param)

scheduler_params = {
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
