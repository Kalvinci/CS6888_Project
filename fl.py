import queryparser as qparser
from replacement import getTruePositiveDocs, getTrueNegativeDoc
from pymongo import MongoClient

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test" port=27017 "mongodb+srv://abhi:abhi@cluster0.bj0f8.mongodb.net/test"
client = MongoClient("mongodb+srv://abhi:abhi@cluster0.bj0f8.mongodb.net/test")

db = client["sample_mflix"]
orig_collection = db["movies"]
primary_key = "_id"

local_client = MongoClient(port=27017)
local_db = client["testdb"]

wrong_query = {
	"$and": [
		{"$and": [
			{"cast": { "$all": ["Leonardo DiCaprio", "Kate Winslet"] }},
			{"countries": { "$all": ["USA"] }},
			{"genres": { "$in": ["Romance"] } }
		]},
		{"$or": [
			{"type": "movie"},
			{"languages": { "$in": ["English"] }},
			{"year": { "$gte" :1990, "$lt": 2020 }},
			{"runtime": {"$gte": 110, "$lt": 200}},
			{"metacritic": {"$gte": 70}}
		]}
	]
}

correct_query = {
	"$and": [
		{"$and": [
			{"cast": { "$all": ["Leonardo DiCaprio", "Kate Winslet"] }},
			{"countries": { "$all": ["USA", "UK"] }},
			{"genres": { "$in": ["Romance"] } }
		]},
		{"$or": [
			{"type": "movie"},
			{"languages": { "$in": ["English"] }},
			{"year": { "$gte" :1990, "$lt": 2020 }},
			{"runtime": {"$gte": 110, "$lt": 200}},
			{"metacritic": {"$gte": 70}}
		]}
	]
}

# client = MongoClient(port=27017)

# 
# orig_collection = db["order"]
# primary_key = "Orderid"

# wrong_query = {"$or": [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]}
# correct_query = {"$or": [{"$and": [{"Year": {"$gt": 2009}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10007"}, {"Discount": 0}]}]}


oracle_Ids = set()
result_Ids = set()

for doc in orig_collection.find(wrong_query, {primary_key: 1}):
	result_Ids.add(doc[primary_key])

for doc in orig_collection.find(correct_query, {primary_key: 1}):
	oracle_Ids.add(doc[primary_key])

superflous = result_Ids.difference(oracle_Ids)
absent = oracle_Ids.difference(result_Ids)

cp_clause_list, clause_map = qparser.parse(wrong_query)

cps = [item["cp"] for item in cp_clause_list]
clause_list = list(clause_map.values())

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

	for clause in clauses:
		field = list(clause.keys())[0]
		clause_str = str(clause)
		for id in superflous.intersection(ids):
			if id in superflous_clausemap:
				if field in superflous_clausemap[id]:
					superflous_clausemap[id][field].add(clause_str)
				else:
					clause_set = set()
					clause_set.add(clause_str)
					superflous_clausemap[id][field] = clause_set
			else:
				clause_set = set()
				clause_set.add(clause_str)
				superflous_clausemap[id] = { field: clause_set }

			if clause_str in sus_counter:
				counter = sus_counter[clause_str]
				counter += 1
				sus_counter[clause_str] = counter
			else:
				sus_counter[clause_str] = 1

for clause in clause_list:
	field = list(clause.keys())[0]
	clause_str = str(clause)
	docids = set()
	for doc in orig_collection.find({"$and": [clause, {primary_key: {"$in": list(absent)}}]}):
		docids.add(doc[primary_key])
	for id in absent.difference(docids):
		if id in absent_clausemap:
			if field in absent_clausemap[id]:
				absent_clausemap[id][field].add(clause_str)
			else:
				clause_set = set()
				clause_set.add(clause_str)
				absent_clausemap[id][field] = clause_set
		else:
			clause_set = set()
			clause_set.add(clause_str)
			absent_clausemap[id] = { field: clause_set }

		if clause_str in sus_counter:
			counter = sus_counter[clause_str]
			counter += 1
			sus_counter[clause_str] = counter
		else:
			sus_counter[clause_str] = 1

print(sus_counter)

print("\nExonerating innocent clauses...")

mut_collection = local_db["mutation"]

truePos = oracle_Ids.intersection(result_Ids)

replacement_docs = getTruePositiveDocs(orig_collection, cps, truePos, primary_key)

for doc in orig_collection.find({primary_key: {"$in": list(superflous)}}):
	print(doc)

	for replacement_doc in replacement_docs:
		id = doc[primary_key]
		for field in superflous_clausemap[id].keys():
			temp = doc[field] if field in doc else None
			doc[field] = replacement_doc[field] if field in replacement_doc else None
			if primary_key == "_id":
				del doc[primary_key]
			mut_collection.insert_one(doc)
			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row[primary_key])
			if len(ret) == 0:
				for clause in superflous_clausemap[id][field]:
					sus_counter[clause] = sus_counter[clause] - 1
			if primary_key == "_id":
				doc[primary_key] = id
			doc[field] = temp
			mut_collection.delete_one({primary_key: id})

replacement_doc = getTrueNegativeDoc(orig_collection, clause_list)

if replacement_doc != None:
	for doc in orig_collection.find({primary_key: {"$in": list(absent)}}):
		id = doc[primary_key]
		for field in absent_clausemap[id].keys():
			temp = doc[field] if field in doc else None
			doc[field] = replacement_doc[field] if field in replacement_doc else None
			if primary_key == "_id":
				del doc[primary_key]
			mut_collection.insert_one(doc)
			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row[primary_key])
			if len(ret) != 0:
				for clause in absent_clausemap[id][field]:
					sus_counter[clause] = sus_counter[clause] - 1
			if primary_key == "_id":
				doc[primary_key] = id
			doc[field] = temp
			mut_collection.delete_one({primary_key: id})

mut_collection.drop()

sus_clauses = []
for clause, count in sus_counter.items():
	if count > 0:
		sus_clauses.append(clause)

print("\nResult: ", sus_clauses)