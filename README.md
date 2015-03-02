# flume-dirtail-source
  
  原理：
    source基于exec source 进行了简化
    对exec source的newSingleThreadExecutor改为多线程并行的，每几个需要监控的文件对应一个execrunnable实例
    tail -F 实现对文件的持续读，log的daily rolling也是没有问题的
    使用apache的vfs进行目录的监控，当文件有增删改时，都有得到一个eventlistener的回调，根据回调的事件对execrunnable进行维护，新增或者删除。

  config
  
    agent.sources.originallog.type = org.apache.flume.source.dirtail.DirTailSource
    agent.sources.originallog.dirPath = /home/peiliping/dev/logs/
    agent.sources.originallog.file-pattern = ^(.*)(\.log)$
    agent.sources.originallog.charset = UTF-8                                （default）
    agent.sources.originallog.batchTimeout = 3000                            （default）
    agent.sources.originallog.bufferCount = 20                               （default）
