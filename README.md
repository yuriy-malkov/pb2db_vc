# pb2db_vc
Protobuf to DB version control


### How to build 
To build .proto file run following command:
```bash
python -m grpc_tools.protoc -I.\example1\protos --python_out=.\example1\generated .\example1\protos\user.proto
```