import copy
import time
from pymongo import MongoClient

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test"
client = MongoClient(port=27017)
db = client.testdb

oracle_map = {}
result_map = {}

for document in db.order.find({ "$or": [{"$and": [{"Year": { "$gt": 2009 }}, {"Price": { "$gt": 100 }}]}, {"$and": [{ "Zipcode": "10007" }, { "Discount": 0 }]}]}, {"_id": 0}):
	oracle_map[document["Orderid"]] = document

for document in db.order.find({ "$or": [{"$and": [{"Year": { "$gt": 2007 }}, {"Price": { "$gt": 100 }}]}, {"$and": [{ "Zipcode": "10008" }, { "Discount": 0 }]}]}, {"_id": 0}):
	result_map[document["Orderid"]] = document

oracle_orderIds = set(oracle_map.keys())
result_orderIds = set(result_map.keys())

superflous = result_orderIds.difference(oracle_orderIds)
absent = oracle_orderIds.difference(result_orderIds)

# query = {"$or": [{"$and": [{"Year": {"$gt": 2009}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10007"}, {"Discount": 0}]}]}

cps = [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]

sus_counter = {}

superflous_clausemap = {}
absent_clausemap = {}

for cp in cps:
	for op in cp:
		ids = set()
		for document in db.order.find(cp, {"Orderid": 1}):
			ids.add(document["Orderid"])
		for id in superflous:
			if id in ids:
				for clause in cp[op]:
					field = list(clause.keys())[0]
					clause = str(clause)
					if id in superflous_clausemap:
						superflous_clausemap[id][field] = clause
					else:
						superflous_clausemap[id] = { field: clause }

					if clause in sus_counter:
						counter = sus_counter[clause]
						counter += 1
						sus_counter[clause] = counter
					else:
						sus_counter[clause] = 1
					
		for id in absent:
			if id not in ids:
				for clause in cp[op]:
					docids = set()
					for doc in db.order.find(clause, {"Orderid": 1}):
						docids.add(doc["Orderid"])
					if id not in docids:
						field = list(clause.keys())[0]
						clause = str(clause)
						if id in absent_clausemap:
							absent_clausemap[id][field] = clause
						else:
							absent_clausemap[id] = { field: clause }

						if clause in sus_counter:
							counter = sus_counter[clause]
							counter += 1
							sus_counter[clause] = counter
						else:
							sus_counter[clause] = 1

# print(sus_counter)

# print(superflous_clausemap, absent_clausemap)

truePos = oracle_orderIds.intersection(result_orderIds)

union_orderIds = oracle_orderIds.union(result_orderIds)

allIds = set()
for document in db.order.find({}, {"Orderid": 1}):
	allIds.add(document["Orderid"])

trueNeg = allIds.difference(union_orderIds)

# print(truePos, trueNeg)

replacement_doc = result_map[2]

mut_collection = db["mutation"]

for id in superflous:
	doc = copy.deepcopy(result_map[id])
	for field in superflous_clausemap[id].keys():
		# print(field)
		temp = doc[field]
		doc[field] = replacement_doc[field]
		# print(doc)
		mut_collection.insert_one(doc)
		time.sleep(2)
		doc[field] = temp
		
		mut_collection.delete_one({"Orderid": id})

mut_collection.drop()
