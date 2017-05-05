import MapReduce
import sys

"""
Relational Join in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: order id
    # value: one row
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    orders= []
    list_items= []
    for v in list_of_values:
      if(v[0] == 'order'):
          orders.append(v)
      else:
          list_items.append(v)
    for o in orders:
        for l in list_items:
            mr.emit(o+l)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
