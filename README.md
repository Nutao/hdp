# hdp
对HDFS Java API的一些简单操作
## hadoop 配置
- hadoop version: 2.7.2
- 3 个节点, 8G RAM + 100G ROM
- 配置环境: Docker

## 实现的功能
### 2017.10.6 
- 实现HDFS在 java 上的删除, 下载, 上传, 打印文件目录以及获取节点信息
### 2017.10.8
- 支持将计算任务提交到yarn
需要用maven打包成jar, 然后将其上传到master节点,再用hadoop jar xxx.jar package.name 运行.如果是本机配置的Hadoop,可以直接使用命令运行
