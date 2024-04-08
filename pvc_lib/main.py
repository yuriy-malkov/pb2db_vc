import os
import argparse
from grpc_tools import protoc
import mariadb
import sys
import importlib


def get_database_tables(cursor):
    cursor.execute("SHOW TABLES")
    return [columns[0] for columns in cursor.fetchall()]


def get_database_fields_names(database_fields):
    return [list(field.keys())[0] for field in database_fields]


def get_primary_keys_from_database(database_fields):
    return [list(field.keys())[0] for field in database_fields if 'PRI' in field.get(list(field.keys())[0], [])]


def get_existing_primary_fields(cursor, db_name, table_name):
    get_primary_fields_query = (f"SELECT COLUMN_NAME "
                                f"FROM INFORMATION_SCHEMA.COLUMNS "
                                f"WHERE TABLE_SCHEMA = '{db_name}' "
                                f"AND TABLE_NAME = '{table_name}' "
                                f"AND COLUMN_KEY = 'PRI'")
    cursor.execute(get_primary_fields_query)
    return [columns[0] for columns in cursor.fetchall()]


def get_database_fields_options(cursor, db_name, table_name):
    result_list: list = []
    get_existing_fields_query = (f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_DEFAULT, IS_NULLABLE, EXTRA, COLUMN_KEY "
                                 f"FROM INFORMATION_SCHEMA.COLUMNS "
                                 f"WHERE TABLE_SCHEMA = '{db_name}' "
                                 f"AND TABLE_NAME = '{table_name}';")
    cursor.execute(get_existing_fields_query)
    for rows in cursor.fetchall():
        filtered_columns = [field for field in rows[1:] if field not in (None, '')]
        # if data type is varchar, combine it with next element to output ex.: varchar(255)
        # then split it into array of elements
        if filtered_columns[0] == 'varchar':
            filtered_columns = f'{filtered_columns[0]}({filtered_columns[1]}), {", ".join(filtered_columns[2:])}'.split(", ")
        filtered_columns_map: map = {rows[0]: filtered_columns}
        result_list.append(filtered_columns_map)

    return result_list


def create_table_if_not_exists(cursor, table_name, fields, module):
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for field in fields:
        db_data_type = field.GetOptions().Extensions[module.dbDataType]
        create_table_query += f"{field.name} {db_data_type}, "
    create_table_query = create_table_query.rstrip(", ") + ")"
    print(f"create_table_query: {create_table_query}")
    cursor.execute(create_table_query)


def get_proto_primary_keys(proto_message, module):
    return [field.name for field in proto_message.fields if field.GetOptions().Extensions[module.primaryKey]]


def create_primary_key_constraint(cursor, table_name, primary_key_fields):
    if primary_key_fields:
        primary_key_columns = ", ".join(primary_key_fields)
        create_primary_key_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_columns})"
        cursor.execute(create_primary_key_query)


def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE {table_name}"
    cursor.execute(drop_table_query)


# Determine the table name based on the Protocol Buffers class name
def get_proto_table_name(module):
    return module.DESCRIPTOR.package


# Get the fields from the Protocol Buffers schema
def get_proto_fields(proto_message):
    return proto_message.fields


#  Get fields names from the Protobuf Buffers schema
def get_proto_fields_names(fields):
    return [field.name for field in fields]


def get_proto_fields_options(proto_fields):
    for proto_field in proto_fields:
        print(f"name: {proto_field.name} and options: {proto_field.GetOptions()}")

    return [proto_option for proto_option in proto_fields]


def update_primary_keys(cursor, table_name, proto_primary_keys: list):
    primary_keys_string = ", ".join(proto_primary_keys)
    update_primary_keys_query = (f"ALTER TABLE {table_name} "
                                 f"DROP PRIMARY KEY, "
                                 f"ADD PRIMARY KEY ({primary_keys_string})")
    cursor.execute(update_primary_keys_query)


def drop_database_fields(cursor, table_name, fields_to_drop: set):
    drop_existing_fields_query = f"ALTER TABLE {table_name} "
    for field in fields_to_drop:
        drop_existing_fields_query += f"DROP COLUMN IF EXISTS {field},"
    drop_existing_fields_query = drop_existing_fields_query.rstrip(",")
    cursor.execute(drop_existing_fields_query)


def add_database_fields(cursor, table_name, fields):
    add_new_fields_query = f"ALTER TABLE {table_name} "
    for field in fields:
        field_name = field.name
        db_options = field.GetOptions().ListFields()  # Get all options as a list of (name, value) pairs
        db_options_map = {option[0].name: option[1] for option in db_options}  # Convert list to a dictionary
        try:
            data_type = db_options_map['dbDataType']
        except KeyError:
            raise ValueError(f"DataType for the {field_name} is missing")
        not_null = 'NOT NULL' if db_options_map.get('notNull', False) else ''
        primary_key = 'PRIMARY KEY' if db_options_map.get('primaryKey', False) else ''
        auto_increment = 'AUTO_INCREMENT' if db_options_map.get('autoIncrement', False) else ''
        default_value = f"DEFAULT {db_options_map['defaultValue']}" if db_options_map.get('defaultValue') else ''
        add_new_fields_query += (f"ADD COLUMN IF NOT EXISTS "
                                 f"{field_name} "
                                 f"{data_type} "
                                 f"{default_value} "
                                 f"{not_null} "
                                 f"{primary_key} "
                                 f"{auto_increment}, ")
    add_new_fields_query = add_new_fields_query.rstrip(", ")
    cursor.execute(add_new_fields_query)


def infer_schema(message_instance):
    # Get the Descriptor for the message instance
    descriptor = message_instance.DESCRIPTOR

    # Access schema information
    for field in descriptor.fields:
        field_name = field.name
        field_type = field.type
        field_label = field.label

        print("Field Name:", field_name)
        print("Field Type:", field_type)
        print("Field Label:", field_label)
        print("----")


def synchronize_tables_with_proto(proto_dbs, connection, module):
    db_name = connection.database
    cursor = connection.cursor()
    database_tables = get_database_tables(cursor)
    proto_db_instance = infer_schema(module.Database)
    print(f"proto_object: {proto_db_instance}")
    proto_table_name = get_proto_table_name(module)

    for proto_db in proto_dbs:
        proto_fields = get_proto_fields(proto_db)
        # TODO might need to move this down, once table creation logic is optimized
        proto_primary_keys: list = get_proto_primary_keys(proto_db, module)
        # TODO Overall idea is to split  table creation, alteration and drop into 3 functions
        # TODO this will make code more simpler, without extra if statements
        if proto_table_name not in database_tables:
            # TODO refactor create_table_if_not_exists() so it just create tables
            create_table_if_not_exists(cursor, proto_table_name, proto_fields, module)
            # TODO deprecate create_primary_key_constraint and move everything under "update keys" function
            create_primary_key_constraint(cursor, proto_table_name, proto_primary_keys)
            connection.commit()
        else:
            proto_fields_names: list = get_proto_fields_names(proto_fields)
            proto_fields_options: list = get_proto_fields_options(proto_fields)

            database_fields_options: list = get_database_fields_options(cursor, db_name, proto_table_name)
            database_fields_names: list = get_database_fields_names(database_fields_options)
            database_primary_keys = get_primary_keys_from_database(database_fields_options)

            print(proto_fields_options)
            print("-------------------------")
            print(database_fields_options)

            # Check if fields were dropped and/or added
            if set(proto_fields_names) != set(database_fields_names):
                database_fields_to_drop: set = set(database_fields_names).difference(set(proto_fields_names))
                drop_database_fields(cursor, proto_table_name, database_fields_to_drop)
                add_database_fields(cursor, proto_table_name, proto_fields)
                connection.commit()

            # Check if primary keys were changed and update
            if set(proto_primary_keys) != set(database_primary_keys):
                update_primary_keys(cursor, proto_table_name, proto_primary_keys)
                connection.commit()


    # Drop tables that are no longer in the schema
    for table_name in database_tables:
        if proto_table_name not in [get_proto_table_name(module) for proto_db in proto_dbs]:
            drop_table(cursor, proto_table_name)
            connection.commit()

    cursor.close()


def generate_protobufs(input_directory, output_directory, include_paths):
    # Loop through .proto files in the input directory
    for file in os.listdir(input_directory):
        if file.endswith(".proto"):
            input_filepath = os.path.join(input_directory, file)
            protoc.main([
                "grpc_tools.protoc",
                f"--proto_path={include_paths}",
                "--python_out",
                output_directory,
                input_filepath
            ])


def main():
    # Generate compiled protobuf files
    parser = argparse.ArgumentParser(description="Generate compiled protobufs for .proto files in a directory")
    parser.add_argument("--parent_directory", help="Parent directory containing 'protos' subdirectory", required=True)
    parser.add_argument("--include_paths", help="Paths to protobuf includes separated by colons/semicolons", required=True)

    args = parser.parse_args()

    parent_directory = os.path.relpath(args.parent_directory)
    input_directory = os.path.join(parent_directory, "protos")
    output_directory = os.path.join(parent_directory, "generated")
    os.makedirs(output_directory, exist_ok=True)
    include_paths = args.parent_directory + os.pathsep + args.include_paths
    print(f"parent_directory: {parent_directory}")
    print(f"input_directory: {input_directory}")
    print(f"output_directory: {output_directory}")
    print(f"include_paths: {include_paths}")
    generate_protobufs(input_directory, output_directory, include_paths)
    generated_protos_directory = os.path.join(output_directory, "protos")
    print(f"generated_protos_directory: {generated_protos_directory}")

    # Connect to MariaDB Platform
    try:
        connection = mariadb.connect(
            user="root",
            password="root",
            host="127.0.0.1",
            port=3306,
            database="test1"
        )
    except mariadb.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


    # Usage
    sys.path.insert(0, generated_protos_directory)
    # Get a list of all .pb2.py files in the directory
    pb2_files = [f for f in os.listdir(generated_protos_directory) if f.endswith("pb2.py")]
    print(f"pb2_files: {pb2_files}")

    # Dynamically import and access classes from each module
    for pb2_file in pb2_files:
        module_name = os.path.splitext(pb2_file)[0]
        print(f"module_name: {module_name}")
        module = importlib.import_module(module_name)
        print(f"module: {module}")
        proto_schema = module.DESCRIPTOR
        print(f"schema: {proto_schema}")
        proto_messages: list = proto_schema.message_types_by_name.values()
        print(f"package: {proto_schema.package}")
        proto_dbs: list = [message for message in proto_messages if message.GetOptions().Extensions[module.dbTable]]
        print(f"synchronizing tables with {module_name}")
        synchronize_tables_with_proto(proto_dbs, connection, module)
        del module

    connection.close()


if __name__ == "__main__":
    main()


