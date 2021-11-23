import queryparser as qparser
from replacement import getTruePositiveDocs, getTrueNegativeDoc
from pymongo import MongoClient, collection

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test"
client = MongoClient(port=27017)

db = client["testdb"]
orig_collection = db["order"]
primary_key = "Orderid"

oracle_map = {}
result_map = {}

wrong_query = {"$or": [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]}
correct_query = {"$or": [{"$and": [{"Year": {"$gt": 2009}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10007"}, {"Discount": 0}]}]}

for document in orig_collection.find(correct_query):
	oracle_map[document[primary_key]] = document

for document in orig_collection.find(wrong_query):
	result_map[document[primary_key]] = document

oracle_Ids = set(oracle_map.keys())
result_Ids = set(result_map.keys())

superflous = result_Ids.difference(oracle_Ids)
absent = oracle_Ids.difference(result_Ids)

cp_clause_list, clause_list = qparser.parse(wrong_query)

cps = [item["cp"] for item in cp_clause_list]

sus_counter = {}

superflous_clausemap = {}
absent_clausemap = {}

print("\nComputing Suspiciousness counters...")
for item in cp_clause_list:
	ids = set()
	cp = item["cp"]
	clauses = item["clauses"]
	for document in orig_collection.find(cp):
		ids.add(document[primary_key])
	
	for id in superflous.intersection(ids):
		for clause in clauses:
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
	
	for id in absent.difference(ids):
		for clause in clauses:
			docids = set()
			for doc in orig_collection.find(clause):
				docids.add(doc[primary_key])
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

print(sus_counter)

print("\nExonerating innocent clauses...")

mut_collection = db["mutation"]

truePos = oracle_Ids.intersection(result_Ids)

replacement_docs = getTruePositiveDocs(orig_collection, cps, truePos, primary_key)

for replacement_doc in replacement_docs:
	for doc in orig_collection.find({primary_key: {"$in": list(superflous)}}):
		id = doc[primary_key]
		for field in superflous_clausemap[id].keys():
			temp = doc[field]
			doc[field] = replacement_doc[field] if field in replacement_doc else None
			mut_collection.insert_one(doc)
			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row[primary_key])
			if len(ret) == 0:
				clause = superflous_clausemap[id][field]
				sus_counter[clause] = sus_counter[clause] - 1
			doc[field] = temp
			mut_collection.delete_one({primary_key: id})

replacement_doc = getTrueNegativeDoc(orig_collection, clause_list)

if replacement_doc != None:
	for doc in orig_collection.find({primary_key: {"$in": list(absent)}}):
		id = doc[primary_key]
		for field in absent_clausemap[id].keys():
			temp = doc[field]
			doc[field] = replacement_doc[field] if field in replacement_doc else None
			mut_collection.insert_one(doc)
			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row[primary_key])
			if len(ret) != 0:
				clause = absent_clausemap[id][field]
				sus_counter[clause] = sus_counter[clause] - 1
			doc[field] = temp
			mut_collection.delete_one({primary_key: id})

mut_collection.drop()

sus_clauses = []
for clause, count in sus_counter.items():
	if count > 0:
		sus_clauses.append(clause)

print("\nResult: ", sus_clauses)