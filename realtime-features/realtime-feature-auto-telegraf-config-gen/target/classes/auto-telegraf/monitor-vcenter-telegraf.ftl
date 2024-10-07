[agent]
  interval = "30s"
  flush_interval = "10s"


[[inputs.exec]]
  commands = [" sh ${telegrafPath}/monitor-vcenter-telegraf.sh"]
  data_format = "influx"


[[outputs.kafka]]
  brokers = [${brokers}]
  topic = "${topic}"
  routing_tag = "partition_key"
  required_acks = 1
  data_format = "json"
  json_transformation = '''
    $merge([{"event_name": name, "event_ts": timestamp,"event_partition_key":tags.vcenter & "_" & tags.moid, "event_tags":tags, "event_fields":fields}])
  '''