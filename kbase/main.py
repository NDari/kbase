from .database import *

def main():
    db.create_tables([Entry, EntryIndex], safe=True)
    add_data()
    EntryIndex.rebuild()
    EntryIndex.optimize()
    res = search("are")
    for i in res:
        print(i)