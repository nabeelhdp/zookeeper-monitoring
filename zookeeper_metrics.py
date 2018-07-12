
import socket
import os
import subprocess
import sys
import urllib
import urllib2
from urllib2 import URLError
import socket
import re
import json
import time
import ConfigParser
from ConfigParser import SafeConfigParser

def get_config_params(config_file):
  try:
    with open(config_file) as f:
      try:
        parser = SafeConfigParser()
        parser.readfp(f)
      except ConfigParser.Error, err:
        print 'Could not parse: %s Exiting', err
        sys.exit(1)
  except IOError as e:
    print "Unable to access %s. Error %s \nExiting" % (config_file, e)
    sys.exit(1)

  ams_collector_host = parser.get('zk_config', 'ams_collector_host')
  ams_collector_port = parser.get('zk_config', 'ams_collector_port')
  if not ams_collector_port.isdigit():
    print "Invalid port specified for AMS Collector. Exiting"
    sys.exit(1)
  if not is_valid_hostname(ams_collector_host):
    print "Invalid hostname provided for AMS collector. Exiting"
    sys.exit(1)

  zk_timeout = parser.get('zk_config', 'zktimeout')
  ams_collector_timeout = parser.get('zk_config', 'ams_collector_timeout')
  if not ams_collector_timeout.isdigit():
    print "Invalid timeout value specified for AMS Collector. Using default of 3 seconds"
    ams_collector_timeout = 3
  if not zk_timeout.isdigit():
    print "Invalid timeout value specified for zookeeper. Using default of 3 seconds"
    zk_timeout = 3

  zkquorum = parser.get('zk_config', 'zkquorum')
  for zkinstance in zkquorum.split(','):
      zkhost,zkport = zkinstance.strip().split(':')
      if not is_valid_hostname(zkhost):
        print "Invalid Quroum - Zookeeper hostname %s is not valid. Exiting!",zkhost
        sys.exit(1)
      if not zkport.isdigit():
        print "Invalid Quroum - Zookeeper host port pair %s:%s not valid. Exiting!",zkhost,zkport
        sys.exit(1)

  # Prepare dictionary object with config variables populated
  config_dict = {}
  config_dict["ams_collector_host"] = ams_collector_host
  config_dict["ams_collector_port"] = ams_collector_port
  config_dict["zkquorum"] = zkquorum
  config_dict["ams_collector_timeout"] = ams_collector_timeout
  config_dict["zk_timeout"] = zk_timeout
  return config_dict


# Read command output from socket and return output
def netcat(host, port, command, timeout):
  try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    s.sendall(command)
    s.shutdown(socket.SHUT_WR)
    data = s.recv(4096).strip();
    return data
  except socket.error, e:
    print "Socket Connection error: %s" % e
    sys.exit(1)

# Identify leader in zookeeper quorum
def get_leader(conn_dict,cmd,timeout):
  for zkinstance in conn_dict["zkquorum"].split(','):
    zkhost,zkport = zkinstance.split(':')
    socket_data = netcat(zkhost,int(zkport),cmd,timeout)
    # Iterate in output for the Mode line to identify current quorum leader
    for kvpair in socket_data.splitlines():
      if "mode" in kvpair.lower():
        if "leader" in kvpair.lower() or "standalone" in kvpair.lower():
          conn_dict["zkhost"] = zkhost
          conn_dict["zkport"] = zkport
          return conn_dict
  print "No zookeeper leader found. Exiting"
  sys.exit(1)

def get_mntr_output(conn_dict,timestamp,timeout):
  mntr_dict = {}
  server_state = {
    1: "follower",
    2:  "leader",
    3:  "standalone"
  }
  # Set a single timestamp for metrics pulled from a single command
  socket_data = netcat(conn_dict["zkhost"],int(conn_dict["zkport"]),'mntr',timeout)
  for kvpair in socket_data.splitlines():
    # Version is an unchanging value. Not required in metrics data. Skip line
    if "version" not in kvpair.lower():
      key,value=kvpair.split('\t')
      if key == "zk_server_state":
        mntr_dict[key]=server_state.get(value,0)
      else:
        mntr_dict[key]=value
  return mntr_dict

# Prepare json object for each of the zk stats in format recognized by Ambari metrics
def construct_metric(key,value,zkleader,timestamp):
    metrics = {}
    vals = {}
    metric_dict = {}
    metrics["hostname"] = zkleader
    metrics["appid"] = "zookeeper"
    metrics["type"]="COUNTER"
    metrics["starttime"] = timestamp
    metrics["timestamp"] = timestamp
    metrics["metricname"] = key
    vals[timestamp] = value
    metrics["metrics"] = vals
    # Construct ambari metrics style json object to insert into AMS Collector
    metric_dict ["metrics"] = [metrics]
    metric_json=json.dumps(metric_dict, indent=4, sort_keys=True)
    return metric_json

def is_valid_hostname(hostname):
    if hostname == "":
        return False
    if len(hostname) > 255:
        return False
    if hostname[-1] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

# Publishing the Metrics to Collector using HTTP call
def publish_metrics(metric_data,ams_collector_host,ams_collector_port,timeout):
    # Test socket connectivity to AMS Collector service port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      s.connect((ams_collector_host,int(ams_collector_port)))
    except Exception as e:
      print("Unable to connect to AMS Collector host %s:%d. Exception is %s\nExiting!" % (ams_collector_host,int(ams_collector_port),e))
      sys.exit(1)
    finally:
      s.close()

    # Submit metrics to AMS Collector
    url = "http://"+ str(ams_collector_host) +":" + str(int(ams_collector_port)) + "/ws/v1/timeline/metrics"
    headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
    req = urllib2.Request(url, metric_data, headers)
    #print metric_data
    try: urllib2.urlopen(req,timeout=timeout)
    except URLError as e:
      print 'Metrics submission failed with error:', e.errno

def main():

  # If config file explicitly passed, use it. Else fall back to zk_config.ini as default filename
  config_file = sys.argv[1] if len(sys.argv) >= 2 else os.path.join(os.path.dirname(__file__),"zk_config.ini")

  # Initialize dictionaries
  mntr_output = {}
  config_dict = {}
  conn_params = {}

  # Read zookeeper connection parameters from configuration filename
  config_dict = get_config_params(config_file)
  # Move zkquorum and zktimeout to separate variables to later pass to the metric construction code
  zkquorum = config_dict["zkquorum"]
  zk_timeout = float(config_dict["zk_timeout"])

  # Identify leader from zookeeper quorum, because specific stats in mntr output are shown only when run against leader
  conn_params = get_leader(config_dict,'stat',zk_timeout)
  zkleader = conn_params["zkhost"]
  # Set a timestamp per iteration as time when we run mntr command
  timestamp = int(time.time()*1000)
  # Run mntr command against leader, return a multiline set of strings as output
  print 'Extracting zookeeper connection statistics'
  mntr_output = get_mntr_output(conn_params,timestamp,zk_timeout)
  # Extract each line from mntr output
  for k,v in mntr_output.iteritems():
    # construct metrics json object as expected by ambari from the key value pairs obtained in mntr output
    metric_data = construct_metric(k,v,zkleader,timestamp)
    # Publish json object to the AMS collector server
    print "Publishing metric data for metric: ",k
    ams_collector_timeout = float(config_dict["ams_collector_timeout"])
    publish_metrics(metric_data,conn_params["ams_collector_host"],conn_params["ams_collector_port"],ams_collector_timeout)

if __name__ == "__main__":
  main()
