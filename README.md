# pb2db_vc
Protobuf to DB version control


### How to build 
To build .proto file from command line run following command:
```bash
python -m grpc_tools.protoc -I.\example1\protos --python_out=.\example1\generated .\example1\protos\user.proto
```

To run migration use following command:
```bash
python main.py --parent_directory example1 --include_paths venv\Lib\site-packages\grpc_tools\_proto
```
