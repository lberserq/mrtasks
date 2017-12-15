import mincemeat
import nltk
import csv
def load_csv(filename):
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile)
        lines = []
        for row in reader:
            lines.append(row)
        return lines[1:] #skip hdr
    #return []


data = load_csv("../data/southpark/All-seasons.csv")
#data = load_csv("a.csv")
# The data source can be any dictionary-like object
datasource = dict(enumerate(data))
#print(datasource)
def mapfn(k, v):
    import nltk
    v[3] = v[3].replace('\n', '')
    v[3] = nltk.tokenize.word_tokenize(v[3])
    v[3] = [x.lower() for x in v[3]]
    v[2] = v[2].split(',')
    #print(v[3])
    for i in v[3]:
        if str.isalpha(i[0]):
            for j in v[2]:
                yield j.strip(), i


def reducefn(k, vs):
    #print(k)
    #print(vs)
    result = len(set(vs))
    return result

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print(results)
with open("result.csv", "w") as f:
    for k,v in results.items():
        f.write("{0}, {1}\n".format(k, str(v)))
