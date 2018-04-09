#!/usr/bin/env python

import sys

from os.path import dirname, realpath 
proj_dir = dirname(dirname(dirname(realpath(__file__))))
sys.path.append(proj_dir+"/scripts/")

from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/PageRankConverge"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33227",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

program_params = {
    "url" : "/datasets/graph/webuk-adj",
    "num_vertices" : 133633040,
    # "url" : "/datasets/graph/google-adj",
    # "num_vertices" : 427554,

    "num_local_threads" : 20,

    "num_parts" : 400,
    "combine_type" : "kDirectCombine",
    "num_iters" : 10,
    "staleness" : 0,
    "pr_url" : "/tmp/tmp/yz/tmp/0409/1930/",
    "topk_url" : "/tmp/tmp/yz/tmp/tmp/10-1/topk",
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

# for i in reversed(xrange(5)):
#     program_params["pr_url"] = "/tmp/tmp/yz/tmp/0409/1930/"+str(i)
#     program_params["topk_url"] = "/tmp/tmp/yz/tmp/0409/topk3/"+str(i)
#     program_params["num_iters"] = 10+i*10
#     l = Launcher(schedulerfile, progfile, hostfile,
#                  common_params, scheduler_params, program_params, env_params,
#                  dump_core)
#
#     l.Launch(sys.argv)
