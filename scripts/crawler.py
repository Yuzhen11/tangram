#!/usr/bin/env python

import sys
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/Crawler"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33224",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

program_params = {
    # "url" : "http://course.cse.cuhk.edu.hk/~csci4140",
    # "url" : "http://www.sina.com.cn",
    "url": "https://en.wikipedia.org/wiki/Main_Page",
    "num_local_threads" : 20,
    "python_script_path" : "/data/yuzhen/xyz/examples/crawler_util.py",
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
