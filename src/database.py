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


from src.functions import query,           get_table,      visualize_table
from src.functions import insert,          update,         delete_entry
from src.functions import init,            get_connection
from src.functions import create_table,    delete_table
from src.functions import table_exists,    entry_exists


class Database:
    """
    A Database object is an additional level of abstraction and simplification over the database module functions
    The methods of this object call the modules' functions and return the same values as them.
    Using the Database object is highly recommended due to its additional functionality and simplicity
    """


    def __init__(self, filename: str):
        conn, cursor = get_connection(filename)
        self.filename          = str(filename)
        init(filename)


    def create_table(self, table_name: str, fields: dict) -> bool:
        return create_table(self.filename, table_name, fields)


    def query(self, table: str, field_name: str, field_value) -> list or bool:
        return query(self.filename, table, field_name, field_value)


    def get_table(self, table: str) -> list or bool:
        return get_table(self.filename, table)


    def insert(self, table: str, values: dict) -> bool:
        return insert(self.filename, table, values)

    
    def visualize_table(self, table: str) -> None:
        return visualize_table(self.filename, table)


    def update(self, table: str, condition: str, attribute: str, new_value) -> bool:
        return update(self.filename, table, condition, attribute, new_value)


    def delete_entry(self, table: str, condition: str) -> bool:
        return delete_entry(self.filename, table, condition)


    def delete_table(self, table: str) -> bool:
        return delete_table(self.filename, table)


    def plot(self) -> bool:
        """
        Plots all of the db's tables using the Database.visualize method and the Database.tables list

        :return: None
        """
        
        if not len(self.tables) > 0:
            print("No table exists in this database")
            return False
        
        for table in self.tables:
            print(f"\n{table}")
            self.visualize_table(table)

        return True


    @property
    def tables(self) -> list:
        _tables = self.get_table("tables")
        return [item[0] for item in _tables]
