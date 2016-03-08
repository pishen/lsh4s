from annoy import AnnoyIndex
from bottle import route, run, request
import time
import random

f = 64
t = AnnoyIndex(f, metric='euclidean')

fo = open("input_vectors", "r")

print "adding items " + str(time.strftime("%H:%M:%S"))
for i, line in enumerate(fo.readlines()):
    v = [float(x) for x in line.split()[1:]]
    t.add_item(i, v)

print "building " + str(time.strftime("%H:%M:%S"))
t.build(128)
print "done " + str(time.strftime("%H:%M:%S"))
t.save('test.ann')

u = AnnoyIndex(f, metric='euclidean')
u.load('test.ann')

# @route('/query/<id>')
# def query(id):
#     return ','.join([str(i) for i in u.get_nns_by_item(int(id), 11)])

# run(host='localhost', port=10023)

ids = range(1450000)
random.shuffle(ids)

for i in ids[:3000]:
    print "querying " + str(i) + " " + str(time.strftime("%H:%M:%S"))
    u.get_nns_by_item(i, 11)

print "done " + str(time.strftime("%H:%M:%S"))
