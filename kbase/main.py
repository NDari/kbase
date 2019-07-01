import os

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


def add_entry(content):
    entry = Entry.create(content=content.lower())
    EntryIndex.create(rowid=entry.id, content=entry.content)

def add_data():
    s = """gantt chart is a type of bar chart that illustrates a project schedule with times at top, and expected task completion as rows. Modern Gantt charts also show the dependency relationships between activities and current schedule status. They do not help determine which schedule is best.
Modern Gantt charts also show the dependency relationships between activities and current schedule status. They do not help determine which schedule is best
Johnson's rule or algorithm tries to minimize idle time between workers which are doing tasks must be done in sequence, such as washing and drying many load of laundary
Johnson's rule is to start the shortest independent tasks first and shortest dependent task last. Sorting tasks like this minimizes idle time and maximizes total work per unit of time
in scheduling algorithms, maximal lateness of a set of tasks is the lateness of the most late task
earliest due date is a scheduling algorithm to reduce maximal lateness. For this, find the task due the soonest, and work your way down to the one due last
in the earliest due date scheduling algorithm, the length of the tasks to be done is completely irrelavant, only when they are due. This the optimal strategy to reduce maximal lateness
the earliest due date scheduling algorithm may cause some/all tasks to be late. It is only minimizing the max lateness of the latest task, but it may make all tasks late by a smaller amount to do so.
Moore-Hodgson algorithm is a scheduling algorithm which tries topd reduce the number of late tasks. For this we start by sorting tasks by earliest due date. If it looks like we wont make a deadline, we remove/delete the most time-consuming task from the queue and repeat. If we cant delete, we can just put these tasks at the end of the queue since they will be late anyway
in scheduling, sum of completion times is the total time that completing a set of tasks takes
Shortest Processing Time or Shortest Job Next is the scheduling algorithm which tries to minimizing sum of completion times. For this, we always do the quickest task we can
Shortest processing time is reccommanded in the book "Getting Things Done" in the format of "do any tasks that are < 2 minutes right away". it allows the completion of the largest possible number of tasks, and minimizing the length of the todo list. it also ignores the weight of tasks, i.e. their importance. An implementation of this algorithm is the "snowball method" of paying the smallest debts first
denial of service (DOS) attack is achieved by flooding a scheduler with many superfluous requests to overwhelm it and prevent it from servicing anyone else
the shortest processing time scheduling algorithm could lead into "hanging" as the quick and easy tasks keep coming in and prioritized at the expense of slower but more important tasks
weighted shortest processing time schedualing algorithm modifies shortest processing time by adding weights to tasks and doing the job with the highest weight to processing time ratio maximizes the value flow per unit of time. it maximizes the value flow per unit of time. An implementation of this algorithm is the "avalanch method" of paying the debt with the highest intereast rate
to set plot size in matplotlib add plt.figure(figsize=(xInches, yInches)) before you plt.plot(thing)
grouping by fns.window(col("timestampColumn"), "1 minute") allows you to bucket by duration, 1 minutes in this example.
in python, to import some random file and use its functions, do import sys; import os; sys.path.append(os.path.abspath("/home/el/foo4/stuff")); from fileInStuff import thing;
to select columns from a csv file do cat file.csv | awk -F "\"*,\"*" ' {printf "%s,\"%s\",\"%s\"\n", $7,$25,$26}'
to sort unique lines based on a column do: sort -u -t, -k1,1 file where -u is for unique, -t, to set comma as delimeter, and k1,1 is for field 1
how-to for spark udaf in https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html
how to mount an ebs volume to an ec2 instance https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html
in pyspark, the error "attribute does not have tzinfo" is usually caused by returning Row objects instead of tuples. as Row is sorted alphabetically
censusdata python library is great to search and dl data. Guild https://jtleider.github.io/censusdata/example1.html"""
    entries = s.split("\n")
    for e in entries:
        add_entry(e)

def add_wiki_data():
    src_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(src_dir, '../data/wikipedia_extracted_abstracts_700000.csv')
    df = pd.read_csv(data_path)
    df.abstract = df.abstract.astype(str)
    for row in df.itertuples():
        add_entry(row.abstract)

def search(*search_strings):
    matches = ' AND '.join([i.lower() for i in search_strings])
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
    res = search("anar*")
    for i in res:
        print()
        print(i)
        
if __name__ == "__main__":
    main()