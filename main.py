import mariadb
import sys
import example1.generated.user_pb2 as user_pb2


def get_existing_tables(cursor):
    cursor.execute("SHOW TABLES")
    return [table[0] for table in cursor.fetchall()]


def get_existing_primary_fields(cursor, db_name, table_name):
    get_primary_fields_query = (f"SELECT COLUMN_NAME "
                                f"FROM INFORMATION_SCHEMA.COLUMNS "
                                f"WHERE TABLE_SCHEMA = '{db_name}' "
                                f"AND TABLE_NAME = '{table_name}' "
                                f"AND COLUMN_KEY = 'PRI'")
    cursor.execute(get_primary_fields_query)
    return [table[0] for table in cursor.fetchall()]


def get_existing_fields(cursor, db_name, table_name):
    get_existing_fields_query = (f"SELECT COLUMN_NAME "
                                 f"FROM INFORMATION_SCHEMA.COLUMNS "
                                 f"WHERE TABLE_SCHEMA = '{db_name}' "
                                 f"AND TABLE_NAME = '{table_name}';")
    cursor.execute(get_existing_fields_query)
    return [table[0] for table in cursor.fetchall()]


def create_table_if_not_exists(cursor, table_name, fields):
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for field in fields:
        db_data_type = field.GetOptions().Extensions[user_pb2.dbDataType]
        create_table_query += f"{field.name} {db_data_type}, "
    create_table_query = create_table_query.rstrip(", ") + ")"
    cursor.execute(create_table_query)


def get_primary_key_fields(message_descriptor):
    return [field.name for field in message_descriptor.fields if field.GetOptions().Extensions[user_pb2.primaryKey]]


def create_primary_key_constraint(cursor, table_name, primary_key_fields):
    if primary_key_fields:
        primary_key_columns = ", ".join(primary_key_fields)
        create_primary_key_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({primary_key_columns})"
        cursor.execute(create_primary_key_query)


def drop_table(cursor, table_name):
    drop_table_query = f"DROP TABLE {table_name}"
    cursor.execute(drop_table_query)


# Determine the table name based on the Protocol Buffers class name
def get_table_name(message_descriptor):
    return message_descriptor.name.lower()


# Get the fields from the Protocol Buffers schema
def get_fields(message_descriptor):
    return message_descriptor.fields


#  Get fields names from the Protobuf Buffers schema
def get_fields_name(fields):
    return [field.name for field in fields]


def update_primary_keys(cursor, table_name, updated_primary_keys):
    update_primary_keys_query = (f"ALTER TABLE {table_name} "
                                 f"DROP PRIMARY KEY, "
                                 f"ADD PRIMARY KEY ({updated_primary_keys})")
    cursor.execute(update_primary_keys_query)


def drop_existing_fields(cursor, table_name, fields_to_drop: set):
    drop_existing_fields_query = f"ALTER TABLE {table_name} "
    for field in fields_to_drop:
        drop_existing_fields_query += f"DROP COLUMN IF EXISTS {field},"
    drop_existing_fields_query = drop_existing_fields_query.rstrip(",")
    cursor.execute(drop_existing_fields_query)


def add_new_fields(cursor, table_name, fields):
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
    print(add_new_fields_query)
    cursor.execute(add_new_fields_query)


def synchronize_tables_with_proto(db_messages, connection):
    db_name = connection.database
    cursor = connection.cursor()
    existing_tables = get_existing_tables(cursor)

    for message_descriptor in db_messages:
        table_name = get_table_name(message_descriptor)
        fields = get_fields(message_descriptor)
        primary_key_fields: list = get_primary_key_fields(message_descriptor)

        if table_name not in existing_tables:
            create_table_if_not_exists(cursor, table_name, fields)
            create_primary_key_constraint(cursor, table_name, primary_key_fields)
            connection.commit()

        else:
            fields_names: list = get_fields_name(fields)
            existing_fields_names: list = get_existing_fields(cursor, db_name, table_name)
            existing_primary_key_fields: list = get_existing_primary_fields(cursor, db_name, table_name)

            # Check if fields were dropped and/or added
            if set(fields_names) != set(existing_fields_names):
                fields_to_drop: set = set(existing_fields_names).difference(set(fields_names))
                drop_existing_fields(cursor, table_name, fields_to_drop)
                add_new_fields(cursor, table_name, fields)
                connection.commit()

            # Check if field attributes were changed
            if set(primary_key_fields) != set(existing_primary_key_fields):
                updated_primary_keys = ", ".join(primary_key_fields)
                update_primary_keys(cursor, table_name, updated_primary_keys)
                connection.commit()

    # Drop tables that are no longer in the schema
    for table_name in existing_tables:
        if table_name not in [get_table_name(msg) for msg in db_messages]:
            drop_table(cursor, table_name)
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
    messages = user_pb2.DESCRIPTOR.message_types_by_name.values()
    db_messages = [message for message in messages if message.GetOptions().Extensions[user_pb2.dbTable]]
    synchronize_tables_with_proto(db_messages, connection)
    connection.close()


if __name__ == "__main__":
    main()


