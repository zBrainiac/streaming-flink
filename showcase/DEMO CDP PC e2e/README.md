


Connect to CML workspace
```
cd infra
./cdswctl login -n mdaeppen -u  https://ml-18a296af-d86.demo-aws.ylcu-atmi.cloudera.site/mdaeppen/vizapp -y <API Key>
```
Start secure endpoint
```
./cdswctl ssh-endpoint -p e2e -m 4 -c 2
```

Setup SSL host
```
Host cdsw-public
    HostName localhost
    IdentityFile ~/.ssh/id_rsa_cml
    User cdsw
    Port 4869
```