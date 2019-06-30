from .database import *

def main():
    db.create_tables([Entry, EntryIndex], safe=True)
    add_data()
    res = search("john*")
    for i in res:
        print(i)