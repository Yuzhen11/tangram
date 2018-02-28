#!/usr/bin/env python

import os
import os.path
from os.path import dirname, join

ssh_cmd = (
  "ssh "
  "-o StrictHostKeyChecking=no "
  "-o UserKnownHostsFile=/dev/null "
)

def launch_nodes(scheduler_path, prog_path, hostfile_path, env_params,
        common_params, scheduler_params, program_params, dump_core):
  assert os.path.isfile(prog_path)
  assert os.path.isfile(hostfile_path)
  assert os.path.isfile(scheduler_path)

   
  clear_cmd = "ls " + hostfile_path + " > /dev/null; ls " + prog_path + " > /dev/null; "
  if dump_core:
      clear_cmd += "ulimit -c unlimited; "
  with open(hostfile_path, "r") as f:
    hostlist = []
    hostlines = f.read().splitlines()
    for line in hostlines:
      if not line.startswith("#"):
        hostlist.append(line) # host

    program_params.update(common_params)
    for host in hostlist:
      print "host:%s" % host
      cmd = ssh_cmd + host + " "  # Start ssh command
      cmd += "\""  # Remote command starts
      cmd += clear_cmd
      # Command to run program
      cmd += env_params + " " + prog_path
      cmd += "".join([" --%s=%s" % (k,v) for k,v in program_params.items()])

      cmd += "\""  # Remote Command ends
      cmd += " &"
      print cmd
      os.system(cmd)

  # run scheduler
  scheduler_params.update(common_params);
  clear_cmd = "ls " + scheduler_path + " > /dev/null; "
  if dump_core:
      clear_cmd += "ulimit -c unlimited; "
  print "Scheduler: node_id:%s, host:%s, port:%s" %(0, scheduler_params["scheduler"], scheduler_params["scheduler_port"])
  cmd = ssh_cmd + scheduler_params["scheduler"] + " "  # Start ssh command
  cmd += "\""  # Remote command starts
  cmd += clear_cmd
  # Command to run program
  cmd += env_params + " " + scheduler_path
  cmd += "".join([" --%s=%s" % (k,v) for k,v in scheduler_params.items()])

  cmd += "\""  # Remote Command ends
  cmd += " &"
  print cmd
  os.system(cmd)


def kill_nodes(scheduler_name, prog_name, hostfile_path):
  # kill scheduler
  # TODO: now only consider scheduler to be in the same node as the script.
  scheduler_name = scheduler_name.split("/")[-1]  # To prevent users give a path to prog
  cmd = "killall -q " + scheduler_name
  os.system(cmd)

  assert os.path.isfile(hostfile_path)
  prog_name = prog_name.split("/")[-1]  # To prevent users give a path to prog
  print "Start killing <%s> according to <%s>" % (prog_name, hostfile_path)

  # Get host IPs
  with open(hostfile_path, "r") as f:
    host_ips = f.read().splitlines()

  for ip in host_ips:
    cmd = ssh_cmd + ip + " killall -q " + prog_name
    os.system(cmd)
  print "Done killing <%s> for <%s>" % (prog_name, hostfile_path)


def parse_file(schedulerfile, progfile, hostfile):
  script_path = os.path.realpath(__file__)
  proj_dir = dirname(dirname(script_path))
  print "flexps porj_dir:", proj_dir
  scheduler_path = join(proj_dir, schedulerfile)
  hostfile_path = join(proj_dir, hostfile)
  prog_path = join(proj_dir, progfile)
  print "scheduler_path:%s, hostfile_path:%s, prog_path:%s" % (scheduler_path, hostfile_path, prog_path)
  return scheduler_path, prog_path, hostfile_path


def launch_util(schedulerfile, progfile, hostfile, env_params, 
        common_params, scheduler_params, program_params, argv, dump_core = False):
  scheduler_path, prog_path, hostfile_path = parse_file(schedulerfile, progfile, hostfile)
  if len(argv) == 1:
    launch_nodes(scheduler_path, prog_path, hostfile_path, env_params,
            common_params, scheduler_params, program_params, dump_core)
  elif len(argv) == 2 and argv[1] == "kill":
    kill_nodes(scheduler_path, prog_path, hostfile_path)
  else:
    print "arg error: " + str(argv)
