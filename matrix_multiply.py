import MapReduce
import sys

"""
Matrix multiplication in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    matrix = record[0]
    row = record[1]
    col = record[2]
    matSize = 5
    if matrix=='a':
        for i in range(0,5):
            key = str(row)+ ',' + str(i)
            mr.emit_intermediate(key, record)
    if matrix == 'b':
        for i in range(0,5):
            key = str(i) + ',' + str(col)
            mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    
    keys = key.split(',')
    a = [0,0,0,0,0]
    b = [0,0,0,0,0]
    for v in list_of_values:
        mat = v[0]
        row = int(v[1])
        col = int(v[2])
        val = int(v[3])
        if mat=='a':
            a[col] = val
        if mat=='b':
            b[row] = val
    total = 0
    for i in range(0,5):
        total = total + (a[i] * b[i])
        
    mr.emit((int(keys[0]),int(keys[1]), total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
