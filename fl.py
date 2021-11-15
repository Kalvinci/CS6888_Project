from pymongo import MongoClient

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test"
client = MongoClient(port=27017)
db = client.testdb

oracle_map = {}
result_map = {}

for document in db.order.find({ "$or": [{"$and": [{"Year": { "$gt": 2009 }}, {"Price": { "$gt": 100 }}]}, {"$and": [{ "Zipcode": "10007" }, { "Discount": 0 }]}]}):
	oracle_map[document["Orderid"]] = document

for document in db.order.find({ "$or": [{"$and": [{"Year": { "$gt": 2007 }}, {"Price": { "$gt": 100 }}]}, {"$and": [{ "Zipcode": "10008" }, { "Discount": 0 }]}]}):
	result_map[document["Orderid"]] = document

oracle_orderIds = set(oracle_map.keys())
result_orderIds = set(result_map.keys())

superflous = result_orderIds.difference(oracle_orderIds)
absent = oracle_orderIds.difference(result_orderIds)

# query = {"$or": [{"$and": [{"Year": {"$gt": 2009}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10007"}, {"Discount": 0}]}]}

cps = [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]

sus_counter = {}

for cp in cps:
	for op in cp:
		ids = set()
		for document in db.order.find(cp, {"Orderid": 1}):
			ids.add(document["Orderid"])
		for id in superflous:
			if id in ids:
				for clause in cp[op]:
					clause = str(clause)
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
					clause = str(clause)
					if id not in docids:
						if clause in sus_counter:
							counter = sus_counter[clause]
							counter += 1
							sus_counter[clause] = counter
						else:
							sus_counter[clause] = 1

print(sus_counter)

mut_collection = db["mutation"]

truePos = oracle_orderIds.intersection(result_orderIds)

union_orderIds = oracle_orderIds.union(result_orderIds)

allIds = set()
for document in db.order.find({}, {"Orderid": 1}):
	allIds.add(document["Orderid"])

trueNeg = allIds.difference(union_orderIds)

print(truePos, trueNeg)