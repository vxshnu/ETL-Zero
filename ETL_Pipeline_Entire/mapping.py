import json
import pandas as pd
from gensim.models import Word2Vec
from sqlalchemy import create_engine, inspect

# ---------------------- DATABASE CONFIGURATION ----------------------
SOURCE_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "raw_db"
}

TARGET_DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "silver_db_mapping"
}

# ---------------------- SIMILARITY THRESHOLD ----------------------
SIMILARITY_THRESHOLD = 0.8

# ---------------------- CONNECT TO DATABASES ----------------------
source_engine = create_engine(
    f"mysql+mysqlconnector://{SOURCE_DB_CONFIG['user']}:{SOURCE_DB_CONFIG['password']}@{SOURCE_DB_CONFIG['host']}/{SOURCE_DB_CONFIG['database']}"
)
target_engine = create_engine(
    f"mysql+mysqlconnector://{TARGET_DB_CONFIG['user']}:{TARGET_DB_CONFIG['password']}@{TARGET_DB_CONFIG['host']}/{TARGET_DB_CONFIG['database']}"
)

# ---------------------- CLASS: DatasetMapper ----------------------
class DatasetMapper:
    def __init__(self):
        self.dataset = pd.DataFrame()
        self.schema_info, self.primary_keys, self.foreign_keys = self.extract_schema()
        self.word2vec_model = self.train_word2vec()

    def extract_schema(self):
        """
        Extracts schema from the MySQL source database, including primary keys and foreign keys.
        Returns:
            schema_info: dict {table_name: [col1, col2, ...]}
            primary_keys: dict {table_name: primary_key_column}
            foreign_keys: dict {table_name: foreign_key_column}
        """
        inspector = inspect(source_engine)
        schema_info = {}
        primary_keys = {}
        foreign_keys = {}

        for table_name in inspector.get_table_names():
            columns = [col["name"] for col in inspector.get_columns(table_name)]
            schema_info[table_name] = columns

            pk = inspector.get_pk_constraint(table_name)['constrained_columns']
            if pk:
                primary_keys[table_name] = pk[0]

            fk = inspector.get_foreign_keys(table_name)
            if fk:
                # For simplicity, assume one foreign key per table.
                foreign_keys[table_name] = fk[0]['constrained_columns'][0]

        return schema_info, primary_keys, foreign_keys

    def train_word2vec(self):
        """
        Trains a Word2Vec model for semantic similarity detection.
        Tokenizes column names by splitting on underscores.
        """
        column_names = []
        for columns in self.schema_info.values():
            column_names.extend(columns)
        tokenized_columns = [col.split("_") for col in column_names]
        model = Word2Vec(sentences=tokenized_columns, vector_size=100, window=5, min_count=1)
        return model

    def compute_similarity(self, col1, col2):
        """
        Computes cosine similarity between two column names using the Word2Vec model.
        Returns a float between 0 and 1.
        """
        try:
            return self.word2vec_model.wv.similarity(col1, col2)
        except KeyError:
            return 0.0

    def find_valid_similar_column(self, columns1, columns2):
        """
        Iterates over columns in two tables and returns a tuple (join_key, similarity)
        if a pair has a similarity score meeting or exceeding SIMILARITY_THRESHOLD.
        Returns (None, 0.0) if no valid match is found.
        """
        best_key = None
        best_score = 0.0
        for col1 in columns1:
            for col2 in columns2:
                sim = self.compute_similarity(col1, col2)
                if sim > best_score and sim >= SIMILARITY_THRESHOLD:
                    best_score = sim
                    best_key = col1
        return best_key, best_score

    def generate_sql(self, table1, table2, join_key):
        """
        Generates a SQL JOIN query to merge table1 and table2 on join_key.
        Aliases columns from each table to avoid duplicate names.
        """
        cols1 = self.schema_info[table1]
        cols2 = self.schema_info[table2]

        select_cols = []
        # For table1: include join_key once; alias other columns with table1 suffix.
        for col in cols1:
            if col == join_key:
                select_cols.append(f"t1.{col} AS {col}")
            else:
                select_cols.append(f"t1.{col} AS {col}_{table1}")
        # For table2: exclude join_key (already included) and alias remaining columns.
        for col in cols2:
            if col != join_key:
                select_cols.append(f"t2.{col} AS {col}_{table2}")

        select_clause = ", ".join(select_cols)
        sql_query = f"SELECT {select_clause} FROM {table1} AS t1 JOIN {table2} AS t2 ON t1.{join_key} = t2.{join_key};"
        return sql_query

    def merge_tables(self):
        """
        Iterates over pairs of tables and attempts to determine a valid join key:
          1. Prioritizes foreign key relationships.
          2. If no FK exists, uses Word2Vec semantic matching with a threshold.
        Verifies that the join key exists in both tables, then generates and executes a SQL JOIN
        query with explicit aliasing. Merged data is stored in the target database.
        Tables with no valid join key are transferred as-is.
        """
        joined_tables = set()

        for table1, columns1 in self.schema_info.items():
            for table2, columns2 in self.schema_info.items():
                if table1 != table2 and (table1, table2) not in joined_tables:
                    join_key = None

                    # 1. Check for foreign key relationship.
                    if table1 in self.foreign_keys and self.foreign_keys[table1] in columns2:
                        join_key = self.foreign_keys[table1]
                    elif table2 in self.foreign_keys and self.foreign_keys[table2] in columns1:
                        join_key = self.foreign_keys[table2]
                    
                    # 2. If no FK, use Word2Vec semantic matching.
                    if not join_key:
                        join_key, sim_score = self.find_valid_similar_column(columns1, columns2)
                        if join_key:
                            print(f"ℹ️ Found join candidate between `{table1}` and `{table2}` using semantic matching (score: {sim_score:.2f}).")

                    # Validate that join_key exists in both tables.
                    if join_key and (join_key in columns1) and (join_key in columns2):
                        sql_query = self.generate_sql(table1, table2, join_key)
                        print(f"✅ Merging `{table1}` with `{table2}` on `{join_key}`")
                        try:
                            with source_engine.connect() as connection:
                                merged_df = pd.read_sql(sql_query, connection)
                                # The SQL query already aliases columns explicitly.
                                merged_df.to_sql(f"{table1}_{table2}_merged", target_engine, if_exists="replace", index=False)
                        except Exception as e:
                            print(f"❌ Error merging {table1} and {table2}: {e}")
                        joined_tables.add((table1, table2))
                        joined_tables.add((table2, table1))
                    else:
                        print(f"⚠️ No valid join key found between `{table1}` and `{table2}`.")

        # Transfer tables that were not merged with any other table.
        for table in self.schema_info.keys():
            if not any(table in pair for pair in joined_tables):
                try:
                    df = pd.read_sql_table(table, source_engine)
                    df.to_sql(table, target_engine, if_exists="replace", index=False)
                    print(f"⚠️ Moving `{table}` as it has no matches.")
                except Exception as e:
                    print(f"❌ Error moving table {table}: {e}")

    def store_dataset(self, path="./utils/stored_dataset.csv"):
        """Stores the mapped dataset to a CSV file."""
        self.dataset.to_csv(path)

# ---------------------- MAIN PROCESS ----------------------
if __name__ == "__main__":
    mapper = DatasetMapper()
    mapper.merge_tables()
    print("\n✅ Data mapping & migration completed successfully.")
