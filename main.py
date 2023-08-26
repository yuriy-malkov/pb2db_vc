import mariadb
import sys
import example1.generated.user_pb2 as user_pb2


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


def create_table_if_not_exists(cursor, table_name, fields):
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for field in fields:
        db_data_type = field.GetOptions().Extensions[user_pb2.dbDataType]
        create_table_query += f"{field.name} {db_data_type}, "
    create_table_query = create_table_query.rstrip(", ") + ")"
    cursor.execute(create_table_query)


def get_proto_primary_keys(proto_message):
    return [field.name for field in proto_message.fields if field.GetOptions().Extensions[user_pb2.primaryKey]]


def create_primary_key_constraint(cursor, table_name, primary_key_fields):
    if primary_key_fields:
        primary_key_columns = ", ".join(primary_key_fields)
        create_primary_key_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_columns})"
        cursor.execute(create_primary_key_query)


def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE {table_name}"
    cursor.execute(drop_table_query)


# Determine the table name based on the Protocol Buffers class name
def get_proto_table_name(proto_message):
    return proto_message.name.lower()


# Get the fields from the Protocol Buffers schema
def get_proto_fields(proto_message):
    return proto_message.fields


#  Get fields names from the Protobuf Buffers schema
def get_proto_fields_names(fields):
    return [field.name for field in fields]


def get_proto_fields_options(proto_fields):
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


def synchronize_tables_with_proto(proto_messages, connection):
    db_name = connection.database
    cursor = connection.cursor()
    database_tables = get_database_tables(cursor)

    for proto_message in proto_messages:
        proto_table_name = get_proto_table_name(proto_message)
        proto_fields = get_proto_fields(proto_message)
        # TODO might need to move this down, once table creation logic is optimized
        proto_primary_keys: list = get_proto_primary_keys(proto_message)

        if proto_table_name not in database_tables:
            # TODO refactor create_table_if_not_exists() so it can add primary keys as well
            create_table_if_not_exists(cursor, proto_table_name, proto_fields)
            # TODO deprecate create_primary_key_constraint
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
        if proto_table_name not in [get_proto_table_name(proto_message) for proto_message in proto_messages]:
            drop_table(cursor, proto_table_name)
            connection.commit()

    cursor.close()


def main():
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
    proto_schema = user_pb2.DESCRIPTOR
    proto_messages: list = proto_schema.message_types_by_name.values()
    proto_messages: list = [message for message in proto_messages if message.GetOptions().Extensions[user_pb2.dbTable]]
    synchronize_tables_with_proto(proto_messages, connection)
    connection.close()


if __name__ == "__main__":
    main()


