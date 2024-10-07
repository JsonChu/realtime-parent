[agent]
  interval = "300s"
  flush_interval = "30s"

[[inputs.vsphere]]
  vcenters = ["${vcenter}"]
  username = "${username}"
  password = "${password}"

  vm_include = ["${paths}"]
  vm_metric_include = [ 
    "cpu.usage.average",
    "cpu.usagemhz.average",	
    "mem.active.average",
    "mem.consumed.average",
    "mem.usage.average",	
    "net.bytesRx.average",
    "net.bytesTx.average",
    "net.usage.average",	
    "disk.read.average",
    "disk.usage.average",
    "disk.write.average",	
    "sys.osUptime.latest",
    "sys.uptime.latest",	
    "datastore.numberReadAveraged.average",
    "datastore.numberWriteAveraged.average"
  ]  
  host_metric_exclude = [ "*" ]
  datastore_metric_exclude = [ "*" ]
  datacenter_metric_exclude = [ "*" ]
  cluster_metric_exclude = [ "*" ]
  resource_pool_metric_exclude = [ "*" ]
  vsan_metric_exclude = [ "*" ]
  insecure_skip_verify = true

[[outputs.kafka]]
  brokers = [${brokers}]
  topic = "${topic}"
  routing_tag = "partition_key"
  required_acks = 1
  data_format = "json"
  json_transformation = '''
    $merge([{"event_name": name, "event_ts": timestamp,"event_partition_key":tags.vcenter & "_" & tags.moid, "event_tags":tags, "event_fields":fields,"vcenter_region":"${region}"}])
  '''
