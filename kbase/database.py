import peewee as pw
from playhouse.sqlite_ext import SqliteExtDatabase, FTSModel, Model, SearchField, RowIDField

pragmas = [
    ('journal_mode', 'wal'),
    ('cache_size', -1024 * 32)]

db = SqliteExtDatabase('/Users/naseerdari/.kbase/main.db', pragmas=pragmas)

class Entry(Model):
    content = pw.TextField()

    class Meta:
        database = db

class EntryIndex(FTSModel):
    rowid = RowIDField()
    content = SearchField()

    class Meta:
        database = db

def add_data():
  entry = Entry.create(
      title='How I rewrote everything with golang'.lower(),
      content='Blah blah blah, type system, channels, blurgh'.lower())
  EntryIndex.create(
      rowid=entry.id,
      content=entry.content)

  entry = Entry.create(
      title='Why ORMs are a terrible idea'.lower(),
      content='Blah blah blah, leaky abstraction, impedance mismatch'.lower())

  EntryIndex.create(
      rowid=entry.id,
      content=entry.content)

def search(*search_strings):
    matches = ' AND '.join([i.lower() for i in search_strings])
    res = (Entry
            .select(Entry.content)
            .join(EntryIndex, on=(Entry.id == EntryIndex.rowid))
            .where(EntryIndex.match(matches))
            .order_by(EntryIndex.bm25())
            .dicts())
    return [i["content"] for i in res]