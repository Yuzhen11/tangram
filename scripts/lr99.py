#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
# progfile = "release/SparseLRExample"
progfile = "release/DenseLRRowExample"
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
    "num_param_per_part" : 16609143/100+1,
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
    "num_param_per_part" : 10000,
}

program_params = {
    "num_local_threads" : 20,
    "num_data_parts" : 400,
    "batch_size" : 350000,  # only for sparse_lr, others are full batch
    "alpha" : 0.1,
    "num_iter" : 10,
    "staleness" : 1,
    # to make FT work, do not use kShuffleCombine,
    # to make it fast, use kShuffleCombine 
    "combine_type" : "kShuffleCombine",
    # "combine_type" : "kNoCombine",
    "max_lines_per_part" : -1,
    "replicate_factor" : 10,
}

# choose one of them
program_params.update(webspam_params)
# program_params.update(a9_params)
# program_params.update(avazu_params)

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

# for machine in [5, 10, 20]:
#     hostfile = "machinefiles/"+str(machine)+"nodes"
#     program_params["num_data_parts"] = machine*20
#     print machine
#     for i in range(3):
#         l = Launcher(schedulerfile, progfile, hostfile,
#                      common_params, scheduler_params, program_params, env_params,
#                      dump_core)
#
#         l.Launch(sys.argv)
