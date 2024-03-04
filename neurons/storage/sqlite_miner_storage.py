import sqlite3

class SqliteMinerStorage:
    
    def __init__(self) -> None:
        """
        This class is responsible for storing data in a sqlite database.
        """
        self.storage_path = "data.sqlite"
        self.init_script = """
        CREATE TABLE IF NOT EXISTS DataRecord (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                url TEXT,
                                search_query TEXT,
                                text TEXT,
                                likes INTEGER,
                                images TEXT,
                                username TEXT,
                                hashtags TEXT,
                                timestamp TEXT)"""
        self.init_storage()
                    
    def init_storage(self):
        """
        Initialize the storage.
        """
        print("Initializing storage")
        conn = sqlite3.connect(self.storage_path)
        c = conn.cursor()
        c.execute(self.init_script)
        conn.commit()
        print("Storage initialized")
        conn.close()
    
    def insert_record(self):
        pass
    
    def insert_records(self, records=[]):
        """
        Insert records into the storage.

        Args:
            records (list, optional): _description_. Defaults to [].
        """
        query = """
            INSERT INTO DataRecord (url, search_query, text, likes, images, username, hashtags, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        records = [tuple(record.values()) for record in records]
        conn = sqlite3.connect(self.storage_path)
        c = conn.cursor()
        c.executemany(query, records)
        conn.commit()
        c.close()
        print(f"Inserted {len(records)} records")

    def query(self, search_queries=[], limit_number=15):
        """
        Query the storage for data.

        Args:
            search_queries (list, optional): A list of search terms to be queried. Defaults to [].
            limit_number (int, optional): The number of records to return. Defaults to 15.

        Returns:
            list: A list of records.
        """
        conn = sqlite3.connect(self.storage_path)
        c = conn.cursor()
        query = """
            SELECT * FROM DataRecord
            WHERE search_query IN {}
            ORDER BY timestamp DESC
            LIMIT {}
        """.format(search_queries, limit_number)
        records = c.execute(query).fetchall()
        print(f"Queried {len(records)} records")
        c.close()
        return records

if __name__=='__main__':
    storage = SqliteMinerStorage()