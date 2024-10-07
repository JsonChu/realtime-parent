[agent]
  interval = "3600s"
  flush_interval = "30s"


[[inputs.exec]]
  commands = [" python3 ${telegrafPath}/vcenter-info.py ${vcenter} ${username} ${password}  "]
  data_format = "influx"
  timeout = "1200s"


[[outputs.kafka]]
  brokers = [${brokers}]
  topic = "${topic}"
  routing_tag = "partition_key"
  required_acks = 1
  data_format = "json"
  json_transformation = '''
    $merge([{"event_name": name, "event_ts": timestamp,"event_partition_key":tags.vcenter & "_" & tags.moid, "event_tags":tags, "event_fields":fields,"vcenter_region":"${region}"}])
  '''
