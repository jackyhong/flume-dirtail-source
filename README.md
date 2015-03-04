# flume-dirtail-source

  用途：

    对一个目录的日志文件进行tail作为flume的source，可以根据文件名进行筛选（正则）
  
  原理：
  
    source基于exec source 进行了简化
    
    对exec source的newSingleThreadExecutor改为多线程并行的，每几个需要监控的文件对应一个execrunnable实例
    
    tail -F 实现对文件的持续读，log的daily rolling也是没有问题的
    
    使用apache的vfs进行目录的监控，当文件有增删改时，都有得到一个eventlistener的回调，根据回调的事件对execrunnable进行维护，新增或者删除。

  config：
  
    agent.sources.originallog.type = org.apache.flume.source.dirtail.DirTailSource
    agent.sources.originallog.dirPath = /home/peiliping/dev/logs/
    agent.sources.originallog.file-pattern = ^(.*)(\.log)$
    agent.sources.originallog.charset = UTF-8                                （default）
    agent.sources.originallog.batchTimeout = 3000                            （default）
    agent.sources.originallog.bufferCount = 20                               （default）
    agent.sources.originallog.topicByFileName = false                        （default）
    agent.sources.originallog.restart = false                                （default）
    agent.sources.originallog.restartThrottle = 10000                        （default）

# TopicInterceptor

  用途：

    对每一个event添加一个header，表示信息的topic
    
    如果用文件的名字，可能不方便管理，容易冲突，这里通过远程加载一个property文件来进行文件名与topic的映射
    
    group字段用于表示不同的配置文件列别，便于管理和维护。

  config：

    agent.sources.originallog.interceptors = topicinterceptor 
    agent.sources.originallog.interceptors.topicinterceptor.type = org.apache.flume.interceptor.TopicInterceptor$Builder
    agent.sources.originallog.interceptors.topicinterceptor.configUri = http://127.0.0.1/
    agent.sources.originallog.interceptors.topicinterceptor.group = das.tmc
    agent.sources.originallog.interceptors.topicinterceptor.checkPeriod = 600000  (default)
