# pb2db_vc
Protobuf to DB version control


### How to build 
To build User.proto file from command line run following command:
```bash
python -m grpc_tools.protoc -I.\db_definitions\protos --python_out=.\db+definitions\generated .\db_definitions\protos\user.proto
```

To run migration use following command:
```bash
python pvc_lib/main.py --parent_directory db_definitions --include_paths venv/lib/python3.10/site-packages/grpc_tools/_proto
```
