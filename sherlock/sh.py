import mincemeat
import nltk
import csv
import os
import string
def load_data(path):
    data={}
    for root,_,files in os.walk(path):
        for filen in files:
            print("loading {0}".format(filen))
            fpath = os.path.join(root, filen)
            with open(fpath, 'r') as f:
                data_item = f.read()
                table = str.maketrans("","", string.punctuation)
                data_item = data_item.translate(table)
                words = [x.lower() for x in nltk.word_tokenize(data_item)]
                data[filen] = words
    return data, files
    #return []


data, files = load_data("../data/sherlock")
files.sort()
def mapfn(k, v):
    
    for word in v:
        yield word, (k, 1, len(v))


def reducefn(k, vs):
    x = {}
    for v in vs:
        doc, val, l = v
        if doc not in x:
            x[doc]=0
        x[doc] += val / float(l)
    return list(set(x.items()))
    #return (k, list(set(vs)))

s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print(results)

with open("result.csv", "w") as f:
    f.write(", " + ", ".join(files) + "\n")
    for k, v in results.items():
        sstr = k
        v.sort(key=lambda x: x[1])
        for fn, val in v:
            if fn in files:
                sstr += ', ' + str(val)
            else:
                sstr += ', 0'
        f.write(sstr + '\n')
