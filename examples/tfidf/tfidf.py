#!/usr/bin/env python

import sys
from os.path import dirname, realpath 
proj_dir = dirname(dirname(dirname(realpath(__file__))))
sys.path.append(proj_dir+"/scripts/")
from launcher import Launcher

hostfile = "machinefiles/20nodes"
progfile = "release/TFIDF2"
schedulerfile = "release/SchedulerMain"

common_params = {
    "scheduler" : "proj99",
    "scheduler_port" : "33254",
    "hdfs_namenode" : "proj99",
    "hdfs_port" : 9000,
}

program_params = {
    # "url" : "/datasets/corpus/enwiki-21g/wiki_0",
    # "url" : "/datasets/corpus/enwiki",
    # "url" : "/datasets/corpus/enwiki-21g",
    # "url" : "/datasets/corpus/enwiki100g",
    "url" : "/datasets/corpus/enwiki-200g-oneline",
    "num_local_threads" : 20,
    "num_of_docs" : 10000,
    "num_doc_partition" : 1000,
    "num_term_partition" : 100,
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
for i in xrange(3):
    l = Launcher(schedulerfile, progfile, hostfile,
                 common_params, scheduler_params, program_params, env_params,
                 dump_core)

    l.Launch(sys.argv)
exit(0)

# for url in ["/datasets/corpus/enwiki50g", "/datasets/corpus/enwiki100g", "/datasets/corpus/enwiki200g"]:
# for url in ["/datasets/corpus/enwiki-200g-oneline"]:
# for url in ["/datasets/corpus/enwiki200g"]:
for url in ["/datasets/corpus/enwiki-50g-oneline", "/datasets/corpus/enwiki-100g-oneline", "/datasets/corpus/enwiki-200g-oneline"]:
    program_params["url"] = url
    for i in xrange(3):
        l = Launcher(schedulerfile, progfile, hostfile,
                     common_params, scheduler_params, program_params, env_params,
                     dump_core)

        l.Launch(sys.argv)
