# pb2db_vc
Protobuf to DB version control

### Setup
You should create venv using following command:
```bash
python3 -m venv pvc_lib/venv
```
Activate it:
```bash
source pvc_lib/venv/bin/activate
```

Setup requirements.txt:
```bash
pip3 install -r pvc_lib/requirements.txt
```

### How to build 
To build User.proto file from command line run following command:
```bash
python -m grpc_tools.protoc -I.\db_definitions\protos --python_out=.\db+definitions\generated .\db_definitions\protos\user.proto
```

To run migration use following command:
```bash
python pvc_lib/main.py --parent_directory db_definitions --include_paths venv/lib/python3.10/site-packages/grpc_tools/_proto
```
