# e2e Demo flow across multiple CDP PC experiences

![e2e demo flow](../../images/showcase_CDP-PC_e2e_flow.png?raw=true "e2e demo flow")


## guideline
1. In Data Hub  
   HUE - upload two files into an s3 bucket  
   a.) lookup table _sensor_io_ to _geo-location - "s3a://demo-aws-2/user/mdaeppen/data_geo"  
   b.) historical iot data - "s3a://demo-aws-2/user/mdaeppen/data_iot"
   

2. In (Data Hub or) CDW  
   HUE - HIVE SQL Editor [follow the script database.sql](database.sql)  
   a.) create database iot  
   b.) create external tables: iot.table_ext_geoloc & iot.table_ext_iot  
   c.) create managed tables: iot.table_managed_geoloc & table_managed_iot  
   d.) create some views & materialized views  
   e.) run some select's
   

3. In CML (browser or remote IDE)  
   Build a PySpark job [sample](ConnectToCDW_SQL.py) in CML that:  
   a.) Shows databases and Tables of Database 'iot'  
   b.) joins the two external tables  
   c.) write result back to s3 - s3a://demo-aws-2/user/mdaeppen/data_out/joined_iot_geoloc  
   
   
4. In CDE  
   Create a new job based on the before writen PySpart job [sample](ConnectToCDW_SQL.py)  


## remote IDE
```
cd infra
./cdswctl login -n <name> -u  https://ml-18a296af-d86.demo-aws.ylcu-atmi.cloudera.site/mdaeppen/vizapp -y <API Key>
```
Start secure endpoint
```
./cdswctl ssh-endpoint -p vizapp -m 4 -c 2
```

Setup SSL host
```
Host cdsw-public
    HostName localhost
    IdentityFile ~/.ssh/id_rsa_cml
    User cdsw
    Port 4869
```