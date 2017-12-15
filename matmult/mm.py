import mincemeat
import nltk
import csv
import os
import string
import random
import math

def gen_matrix(fname, N, prefix):
    
    sstr = ""
    for i in range(N):
        for j in range(N):
            sstr += prefix + ', ' + str(i) + ', ' +  str(j) + ', ' + str(random.randint(0, 100)) + '\n'
    return sstr

def gen_file(filename, N):
    with open(filename, "w") as f:
        f.write(gen_matrix(filename, N, 'a'))
        f.write(gen_matrix(filename, N, 'b'))
        

N=2
gen_file("data.csv", N)
        
def load_data(fname):
    data = []
    ddata = {}
    with open(fname, "r") as f:
        lines = f.read().split('\n')
        for i in range(len(lines)):
            lines[i] = lines[i].split(',')
            lines[i] = [x.strip() for x in lines[i]]
            if (len(lines[i]) <= 1) or not str.isalnum(lines[i][0]):
                continue
            if lines[i][0] not in ddata.keys():
                ddata[lines[i][0]] = []
            ddata[lines[i][0]].append((lines[i][1], lines[i][2], lines[i][3]))
        return ddata



data = load_data("data.csv")
print(data)
def mapfn(k, v):
    import math
    N = int(math.sqrt(len(v)))
    v.sort()
    if k == 'a':
        for t in range(N):
            for it in v:
                i, j, val = it
                yield (i,t), (k,j,val)
    else:
        for i in range(N):
            for it in v:
                j,t, val = it
                yield (i, t), (k, j, val)
            

def reducefn(k, vs):
    avs = list(filter(lambda x: x[0] == 'a', vs))
    bvs = list(filter(lambda x: x[0] == 'b', vs))
    avs.sort(key = lambda x: x[1])
    bvs.sort(key = lambda x: x[1])
    res = 0
    for i in range(len(avs)):
        _,_,av = avs[i]
        _,_,bv = bvs[i]
        res += av * bv
    return (k,res)

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

with open("result.csv", "w") as f:
    sstr = ''
    for it in sorted(results.values()):
        idx, val = it
        i,j = idx
        sstr += str(val) + ', '
        if j == N - 1:
            sstr += '\n'
    f.write(sstr)
