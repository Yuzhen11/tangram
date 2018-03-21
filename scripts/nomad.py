#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/5nodes"
progfile = "release/Nomad"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj10",
    "scheduler_port" : "38214",
    "hdfs_namenode" : "proj10",
    "hdfs_port" : 9000,
}

toy_data = {
    "url" : "/yuzhen/als_toy.txt",
    "kNumUser" : 3,
    "kNumItem" : 3,
    "eta" : 0.01,
    "lambda" : 0.05,
    "num_line_per_part": -1,
    "kNumPartition" : 2,
    "iter": 400, 
    "staleness": 0,
}

# seems that the eta should be small to make it converge.
# for all ratings (10^8), set the eta to 5*10^-5, train 
# for 400 iters, num_partition 20, mse can be 2.4.
netflix_data = {
    "url" : "/datasets/ml/netflix",
    "kNumUser" : 480189,
    "kNumItem" : 17770,
    # "kNumRating" : 100480507,
    "eta" : 0.00005,  # 5*10^-5
    "lambda" : 0.01,
    "num_line_per_part": -1,
    "kNumPartition" : 20,
    "iter": 400, 
    "staleness": 0,
}

netflix_data2 = {
    "url" : "/datasets/ml/netflix",
    "kNumUser" : 480189,
    "kNumItem" : 17770,
    # "kNumRating" : 100480507,
    "eta" : 0.0005,
    "lambda" : 0.01,
    "num_line_per_part": -1,
    "kNumPartition" : 200,
    "iter": 400, 
    "staleness": 0,
}

debug = {
    "url" : "/datasets/ml/netflix",
    "kNumUser" : 480189,
    "kNumItem" : 17770,
    # "kNumRating" : 100480507,
    "eta" : 0.00005,
    "lambda" : 0.01,
    "num_line_per_part": -1,
    "kNumPartition" : 20,
    "iter": 400, 
    "staleness": 100,
}

dataset_param = debug 
# dataset_param = toy_data 

program_params = {
    "num_local_threads": 20,
    "backoff_time" : 100,
    "max_sample_item_size_each_round": 1000,
}
program_params.update(dataset_param)

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
