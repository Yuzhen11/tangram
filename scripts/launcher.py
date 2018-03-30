#!/usr/bin/env python

import os
import os.path
from os.path import dirname, join
import subprocess
import signal

ssh_cmd = (
  "ssh "
  "-o StrictHostKeyChecking=no "
  "-o UserKnownHostsFile=/dev/null "
)

class Launcher:
  def __init__(self, schedulerfile, progfile, hostfile, 
          common_params, scheduler_params, program_params, env_params,
          dump_core=False):
    self.schedulerfile = schedulerfile
    self.progfile = progfile 
    self.hostfile = hostfile 
    self.common_params = common_params 
    self.scheduler_params = scheduler_params 
    self.program_params = program_params 
    self.env_params = env_params
    self.parse_file()
    self.dump_core = dump_core

    signal.signal(signal.SIGINT, self.kill_signal)

  def DebugString(self):
    attrs = vars(self)
    print '\n'.join("%s: %s" % item for item in attrs.items())

  def parse_file(self):
    self.script_path = os.path.realpath(__file__)
    self.proj_dir = dirname(dirname(self.script_path))
    self.scheduler_path = join(self.proj_dir, self.schedulerfile)
    self.hostfile_path = join(self.proj_dir, self.hostfile)
    self.prog_path = join(self.proj_dir, self.progfile)

    # parse the machine file
    assert os.path.isfile(self.hostfile_path)
    with open(self.hostfile_path, "r") as f:
      self.hostlist = []
      hostlines = f.read().splitlines()
      for line in hostlines:
        if not line.startswith("#"):
          self.hostlist.append(line) # host

  def Launch(self, argv):
    if (len(argv) == 1):
      self.launch_workers()
      self.launch_scheduler()
    elif len(argv) == 2 and argv[1] == "kill":
      self.Kill()
    else:
      print "arg error: " + str(argv)

  def kill_signal(self, signum, frame):
    self.Kill()

  def Kill(self):
    # kill scheduler
    # TODO: now only consider scheduler to be in the same node as the script.
    scheduler_name = self.scheduler_path.split("/")[-1]  # To prevent users give a path to prog
    cmd = "killall -q " + scheduler_name
    os.system(cmd)

    prog_name = self.prog_path.split("/")[-1]  # To prevent users give a path to prog
    print "Start killing <%s> according to <%s>" % (prog_name, self.hostfile_path)

    for host in self.hostlist:
      cmd = ssh_cmd + host + " killall -q " + prog_name
      os.system(cmd)
      
    print "Done killing <%s> for <%s>" % (prog_name, self.hostfile_path)

  def launch_workers(self):
    assert os.path.isfile(self.prog_path)
     
    clear_cmd = " ls " + self.prog_path + " > /dev/null; "
    if self.dump_core:
      clear_cmd += "ulimit -c unlimited; "
  
    self.program_params.update(self.common_params)
    for host in self.hostlist:
      print "host:%s" % host
      cmd = ssh_cmd + host + " "  # Start ssh command
      cmd += "\""  # Remote command starts
      cmd += clear_cmd
      # Command to run program
      cmd += self.env_params + " " + self.prog_path
      cmd += "".join([" --%s=%s" % (k,v) for k,v in self.program_params.items()])
  
      cmd += "\""  # Remote Command ends
      cmd += " &"
      print cmd
      os.system(cmd)

  def launch_scheduler(self):
    self.scheduler_params.update(self.common_params);
    self.scheduler_params["num_worker"] = len(self.hostlist)

    clear_cmd = "ls " + self.scheduler_path + " > /dev/null; "
    if self.dump_core:
        clear_cmd += "ulimit -c unlimited; "
    print "Scheduler: node_id:%s, host:%s, port:%s" %(0, self.scheduler_params["scheduler"], self.scheduler_params["scheduler_port"])
    cmd = ssh_cmd + self.scheduler_params["scheduler"] + " "  # Start ssh command
    cmd += "\""  # Remote command starts
    cmd += clear_cmd
    # Command to run program
    cmd += self.env_params + " " + self.scheduler_path
    cmd += "".join([" --%s=%s" % (k,v) for k,v in self.scheduler_params.items()])

    cmd += "\""  # Remote Command ends
    # cmd += " &"
    print cmd
    # os.system(cmd)

    process = subprocess.Popen(cmd, shell=True)
    process.wait()
