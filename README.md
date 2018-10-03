# Zookeeper monitoring

This script will contact the zookeeper leader from a given quorum, poll the mntr output, and publish it to an ambari metrics database in a given Ambari metrics collector host. The script can be run as standalone if you want to test it out. Update the zk_config.ini with the corresponding values in your environment. If you want to pass a different file other than zk_config.ini, use that as an argument when invoking the script.

Example:
```
*/opt/zkmonitoring/scripts/zookeeper_metrics.py
```
OR
```
*/opt/zkmonitoring/scripts/zookeeper_metrics.py my_config.ini
```
If successful, this will publish the metrics data into ambari metrics database (AMS HBase). Once the publishing is successful, the metrics can be viewed in Grafana as explained later.

# What you need to do with the scripts

For continous monitoring of the zookeeper metrics, place the script and the config file on any node in the cluster (prefereably the Ambari metrics collector host itself, provided it can access the zookeeper port on all servers in the qurorum) and add a crontab entry (crontab –e) to schedule it run. 

Example: 
```
*/5 * * * * /opt/zkmonitoring/scripts/zookeeper_metrics.py
```

The above crontab will execute the script every 5 minutes and publish the metrics to AMS collector.

# Steps to create the Grafana dashboard to retrieve the custom metrics we collected.

* Login to Grafana as admin.
* Click on top Dashboard drop down menu -> +New
* Place your cursor on Green hidden tab -> Add Panel -> Graph alt text
* Fill the Metrics details
* Component Name : Zookeeper
* Metrics Name: select one from the metrics drop down menu (Anything beginning with zk_ . Eg. zk_num_alive_connections).
* Select Aggregator as none
* You can select Transform as diff or none, depending on requirement. 
* You can rename the panel.
* Follow the same to add more panels/graphs for each metric name.
