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
Setup "generated" folder, which is a local folder that will store generated DDLs.
```bash
mkdir -p ./db_definitions/generated/protos
```

To build User.proto file from command line run following command:
```bash
python3 -m grpc_tools.protoc -I ./db_definitions/protos --python_out=./db_definitions/generated/protos ./db_definitions/protos/User.proto
```

To run migration use following command:
```bash
python3 pvc_lib/main.py --parent_directory db_definitions --include_paths pvc_lib/venv/lib/python3.10/site-packages/grpc_tools/_proto
```
