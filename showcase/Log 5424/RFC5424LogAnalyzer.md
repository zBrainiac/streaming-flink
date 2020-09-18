RFC 5424 log file generator:

https://github.com/mingrammer/flog

```
wget https://github.com/mingrammer/flog/releases/download/v0.4.2/flog_0.4.2_linux_amd64.tar.gz &&
tar -xf flog_0.4.2_linux_amd64.tar.gz &&
./flog -f rfc5424 -d 1 -l -o /tmp/syslog.log -t log -w
```
Check result of flog:
```
tail -f /tmp/syslog.log
```



Ready to use NiFi flow:  
Template: [link](Log5424.xml)