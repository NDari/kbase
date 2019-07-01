import os
import sys
from datetime import datetime

import peewee as pw
from playhouse.sqlite_ext import SqliteExtDatabase, FTSModel, Model, SearchField, RowIDField
import pandas as pd

def _recreate_db():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(src_dir, '../data/main.db')
    if os.path.exists(db_path):
        os.remove(db_path)
    pragmas = [
        ('journal_mode', 'wal'),
        ('cache_size', -1024 * 32)]

    return SqliteExtDatabase(db_path, pragmas=pragmas)

DB = _recreate_db()

class Entry(Model):
    content = pw.TextField()

    class Meta:
        database = DB

class EntryIndex(FTSModel):
    rowid = RowIDField()
    content = SearchField()

    class Meta:
        database = DB
        options = {'content': Entry.content}

def add_entry(content):
    entry = Entry.create(content=content)
    EntryIndex.create(rowid=entry.id, content=entry.content)

def add_wiki_data():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(src_dir, '../data/wikipedia_extracted_abstracts_700000.csv')
    df = pd.read_csv(data_path)
    df.abstract = df.abstract.astype(str)
    for i, row in enumerate(df[:100000].itertuples()):
        if i % 10000 == 0:
            print(f"done with {i} rows")
        add_entry(row.abstract)

def search(*search_strings):
    matches = ' AND '.join(search_strings)
    res = (Entry
            .select(Entry.content)
            .join(EntryIndex, on=(Entry.id == EntryIndex.rowid))
            .where(EntryIndex.match(matches))
            .order_by(EntryIndex.bm25())
            .dicts())
    return [i["content"] for i in res]

def main():
    DB.create_tables([Entry, EntryIndex], safe=True)
    add_wiki_data()
    res = search(*sys.argv[1:])
    for i in res:
        print()
        print(i)
        
if __name__ == "__main__":
    main()