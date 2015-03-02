# flume-dirtail-source

  config
  
    agent.sources.originallog.type = org.apache.flume.source.dirtail.DirTailSource
    agent.sources.originallog.dirPath = /home/peiliping/dev/logs/
    agent.sources.originallog.file-pattern = ^(.*)(\.log)$
    agent.sources.originallog.charset = UTF-8                                （default）
    agent.sources.originallog.batchTimeout = 3000                            （default）
    agent.sources.originallog.bufferCount = 20                               （default）
