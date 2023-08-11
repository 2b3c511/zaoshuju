# 简介
	一个向iotdb服务写入数据的小程序
# 修改配置
```
# ip:port;ip:port
urls=12.0.0.1:6667
# name顺序对应上面的ip端口
username=root
# pwd顺序对应上面的ip端口
password=123

```
# 编译
	maven clean package 
# 启动
	通过如下命令将jar包挂载到后台运行:
```
java -jar iotdb-real-time.jar
或：
nohup java -jar iotdb-real-time.jar  >/dev/null 2>&1 &
```
