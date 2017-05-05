import MapReduce
import sys

"""
Inverted Index in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: word in the document
    # value: document identifier
    value = record[0]
    keys = record[1]
    keywords = keys.split()
    for w in keywords:
      mr.emit_intermediate(w, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of doc ids
    listdocids = []
    for v in list_of_values:
        if (v not in listdocids):
            listdocids.append(v)
    mr.emit((key, listdocids))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
