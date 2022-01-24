# ***********************************************************************
# SQLAbstract
# Copyright 2022 Alessandro Salerno

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ***********************************************************************


import sqlite3
from   texttable import Texttable


#################################################
#
#               Conversion
# 
#################################################


def condition_to_var(condition):
    condition_list = condition.replace("'", "").replace('"', '').split("=")
    return condition_list[0], condition_list[1]



#################################################
#
#               Functions
# 
#################################################


def get_connection(filename: str):
    """
    Creates a connection to the .db file if it exists
    Creates the .db file if it doesn't exist

    :param filename: Name of the file you'd like to use to store the DB

    :return: connection, cursor
    """

    conn = sqlite3.connect(str(filename))
    c    = conn.cursor()
    return conn, c


def init(filename: str, debug=False) -> bool:
    conn, curosr = get_connection(filename)
    
    try:
        curosr.execute("""CREATE TABLE tables (
                            name text,
                            colums text
                        )""")

        insert(filename, "tables", {'name': "tables", 'colums': "['name', 'colums']"})

        return True
    except: return False


def create_table(filename: str, table_name: str, fields: dict) -> bool:
    """
    Creates an SQL table inside a given database

    :param filename: .db filename (String)
    :param table_name: Name of the table you'd like to create (String)
    :param fields: List of variable fields and types (Dict {'var_name': 'type'})

    :return: Bool
    """

    conn, cursor = get_connection(filename)
    command      = f"""CREATE TABLE {table_name} (\n"""

    for index, field in enumerate(fields):
        if index == len(fields.keys()) - 1:
            command += f"{field} {fields[field]}\n)"
            break

        command += f"{field} {fields[field]},\n"

    if table_exists(filename, table_name):
        return False

    cursor.execute(command)
    header       = str(list(fields.keys()))
    insert(filename, "tables", {'name': table_name, 'colums': header})
    return True


def table_exists(filename: str, table: str) -> bool:
    conn, cursor = get_connection(filename)
    return len(query(filename, "tables", "name", str(table))) > 0


def entry_exists(filename: str, table: str, entry: list) -> bool:
    table_elements = get_table(filename, table)
    return tuple(entry) in table_elements


def get_table(filename: str, table: str) -> list:
    conn, cursor = get_connection(filename)
    
    if not table_exists(filename, table):
        return []

    cursor.execute(f"SELECT * FROM {table}")
    return cursor.fetchall()


def query(filename: str, table: str, field_name: str, filed_value) -> list:
    """
    Returns a list of tuples of values that contains the specified value

    :param filename: .db filename (String)
    :param table: Name of the table (String)
    :param field_name: Name of the field in the table (String)
    :param field_value: Value that the query should look for in the field

    :return: List
    """

    conn, cursor = get_connection(filename)

    cursor.execute(f"SELECT * FROM {table} WHERE {field_name}=:field_value", {'field_value': filed_value})
    return cursor.fetchall()


def insert(filename: str, table: str, values: dict) -> bool:
    """
    Adds an entry to a given table

    :param filename: .db file name (String)
    :param table: Name of the table (String)
    :param values: Dictionary of values (Dict)

    :return: Bool
    """

    conn, cursor = get_connection(filename)
    _values      = [f":{value}" for value in values]
    command      = f"INSERT INTO {table} VALUES {_values}".replace("[", "(").replace("]", ")")
    
    for value in values:
        command = command.replace(f"':{value}'", f":{value}")

    if not table_exists(filename, table):
        if table is not "tables":
            return False

    if entry_exists(filename, table, list(values.values())):
        return False

    cursor.execute(command, values)
    conn.commit()
    return True


def visualize_table(filename: str, table: str) -> bool:
    """
    Formats the contents of a db table using the texttable package

    :param filename: .db file name (String)
    :param table: Name of the table to plot (String)

    :return: Bool
    """

    conn, cursor   = get_connection(filename)
    table_elements = get_table(filename, table)

    if not len(table_elements) > 0:
        print("This table is empty")
        return False

    text_table     = Texttable()
    allign         = ["l" for i in range(len(table_elements[0]))]
    vallign        = ["m" for i in range(len(table_elements[0]))]
    title          = eval(query(filename, "tables", "name", table)[0][1])

    text_table.set_cols_align(allign)
    text_table.set_cols_valign(vallign)
    text_table.header(title)

    for row in table_elements:
        text_table.add_row(row)

    print(text_table.draw())
    return True


def update(filename: str, table: str, condition: str, attribute: str, new_value) -> bool:
    """
    Edits the value at the given table, condition and attribute

    :param filename: .db file name (String)
    :param table: Name of the table (String)
    :param condition: SQL 'WHERE' condition (String: "colum = value")

    :return: Bool
    """

    conn, cursor            = get_connection(filename)
    field_name, field_value = condition_to_var(condition)

    if not len(query(filename, table, field_name, field_value)) > 0:
        return False
    
    cursor.execute(f"UPDATE {table} SET {attribute} = ? WHERE {condition};", (new_value,))
    conn.commit()
    return True


def delete_entry(filename: str, table: str, condition: str) -> bool:
    """
    Deletes an entry from a given table with a given condition

    :param filename: .db file name (String)
    :param table: Name of the table (String)
    :param condition: SQL 'WHERE' condition (String: "colum = value")

    :return: Bool
    """

    conn, cursor            = get_connection(filename)
    field_name, field_value = condition_to_var(condition)

    if not len(query(filename, table, field_name, field_value)) > 0:
        return False
    
    cursor.execute(f"DELETE FROM {table} WHERE {condition}")
    conn.commit()
    return True


def delete_table(filename: str, table: str) -> bool:
    """
    Deletes the specified table

    :param filename: .db file name (String)
    :param table: Name of the table you'd like to delete (String)

    :return: Bool
    """

    conn, cursor = get_connection(filename)

    if not table_exists(filename, table):
        return False

    if table == "tables":
        return False

    cursor.execute(F"DROP TABLE {table}")
    delete_entry(filename, "tables", f"name='{table}'")
    return True
