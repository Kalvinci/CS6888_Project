import re
import queryparser as qparser
from replacement import getTruePositiveDocs, getTrueNegativeDoc
from pymongo import MongoClient
from pprint import pprint

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test" port=27017 "mongodb+srv://abhi:abhi@cluster0.bj0f8.mongodb.net/test"
client = MongoClient(port=27017)

db = client["testdb"]
orig_collection = db["order"]
primary_key = "Orderid"

wrong_query = {"$or" : [
            {"$and" : [
                        {"Year" : { "$gt" : 2007} }, {"Price" : {"$gt" : 100}}
                        ]},
            {"$and" : [
                        {"Zipcode" : "10008"},{"Discount" : 0}
                        ]}
            ]}
correct_query = {"$or" : [
            {"$and" : [
                        {"Year" : { "$gt" : 2009} }, {"Price" : {"$gt" : 100}}
                        ]},
            {"$and" : [
                        {"Zipcode" : "10007"},{"Discount" : 0}
                        ]}
            ]}

# client = MongoClient("mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test")

# db = client["sample_mflix"]
# orig_collection = db["movies"]
# primary_key = "_id"

# wrong_query = {"$and" : [{"year" : {"$gt" : 1900}}, {"$or": [{"runtime" : {"$lt" : 10}}, {"$and" : [{"rated": "APPROVED"},{"runtime" : {"$gt" : 50}}]}]}]}
# correct_query = {"$and" : [{"year" : {"$gt" : 1900}}, {"$or": [{"runtime" : {"$lt" : 10}}, {"$and" : [{"rated": "APPROVED"},{"runtime" : {"$gt" : 60}}]}]}]}

oracle_Ids = set()
result_Ids = set()

for doc in orig_collection.find(wrong_query, {primary_key: 1}):
	result_Ids.add(doc[primary_key])

print(result_Ids)

for doc in orig_collection.find(correct_query, {primary_key: 1}):
	oracle_Ids.add(doc[primary_key])

print(oracle_Ids)


superflous = result_Ids.difference(oracle_Ids)
absent = oracle_Ids.difference(result_Ids)

cp_clause_map, g_clause_map = qparser.parse(wrong_query)
correct_cp_clause_map, correct_g_clause_map = qparser.parse(correct_query)

correct_cps = [item["CP"] for item in correct_cp_clause_map.values()]
correct_clause_list = list(correct_g_clause_map.values())

sus_counter = {}

superflous_clausemap = {}
absent_clausemap = {}

print("\nComputing Suspiciousness counters...")
for item in cp_clause_map.values():
	pass_cp_ids = set()
	cp = item["CP"]
	clause_map = item["clauses"]

	for document in orig_collection.find({"$and": [cp, {primary_key: {"$in": list(superflous)}}]}):
		pass_cp_ids.add(document[primary_key])
	
	print(pass_cp_ids)

	for id in pass_cp_ids:
		for clause_id, clause in clause_map.items():
			field = list(clause.keys())[0]
			clause_str = str(clause)
			
			if id in superflous_clausemap:
				superflous_clausemap[id].append({"field": field, "clause_str": clause_str})
			else:
				superflous_clausemap[id] = [{"field": field, "clause_str": clause_str}]

			if clause_str in sus_counter:
				counter = sus_counter[clause_str]
				counter += 1
				sus_counter[clause_str] = counter
			else:
				sus_counter[clause_str] = 1

	fail_cp_ids = absent.difference(pass_cp_ids)
	for clause_id, clause in clause_map.items():
		field = list(clause.keys())[0]
		clause_str = str(clause)
		pass_clause_ids = set()
		
		for doc in orig_collection.find({"$and": [clause, {primary_key: {"$in": list(absent)}}]}):
			pass_clause_ids.add(doc[primary_key])
		
		fail_clause_ids = fail_cp_ids.difference(pass_clause_ids)
		for id in fail_clause_ids:
			if id in absent_clausemap:
				absent_clausemap[id].append({"field": field, "clause_str": clause_str})
			else:
				absent_clausemap[id] = [{"field": field, "clause_str": clause_str}]

			if clause_str in sus_counter:
				counter = sus_counter[clause_str]
				counter += 1
				sus_counter[clause_str] = counter
			else:
				sus_counter[clause_str] = 1

print(sus_counter)

print("\nExonerating innocent clauses...")

local_client = MongoClient(port=27017)
local_db = local_client["testdb"]
mut_collection = local_db["mutation"]

truePos = oracle_Ids.intersection(result_Ids)

replacement_docs = getTruePositiveDocs(orig_collection, correct_cps, truePos, primary_key)

for doc in orig_collection.find({primary_key: {"$in": list(superflous)}}):
	for replacement_doc in replacement_docs:
		id = doc[primary_key]
		for c_map in superflous_clausemap[id]:
			field = c_map["field"]
			clause_str = c_map["clause_str"]
			
			insert_doc = {}
			for key in doc:
				if key == "_id":
					continue
				if key == field:
					insert_doc[key] = replacement_doc[field] if field in replacement_doc else None
				else:
					insert_doc[key] = doc[key]
			
			insert_result = mut_collection.insert_one(insert_doc)
			insert_id = insert_result.inserted_id

			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row["_id"])
			
			if len(ret) == 0:
				sus_counter[clause_str] = sus_counter[clause_str] - 1
			
			del_result = mut_collection.delete_one({primary_key: id})

replacement_doc = getTrueNegativeDoc(orig_collection, correct_clause_list)

if replacement_doc != None:
	for doc in orig_collection.find({primary_key: {"$in": list(absent)}}):
		id = doc[primary_key]
		for c_map in absent_clausemap[id]:
			field = c_map["field"]
			clause_str = c_map["clause_str"]
			
			insert_doc = {}
			for key in doc:
				if key == "_id":
					continue
				if key == field:
					insert_doc[key] = replacement_doc[field] if field in replacement_doc else None
				else:
					insert_doc[key] = doc[key]

			insert_result = mut_collection.insert_one(insert_doc)
			insert_id = insert_result.inserted_id
			
			ret = set()
			for res_row in mut_collection.find(correct_query):
				ret.add(res_row["_id"])

			if len(ret) != 0:
				sus_counter[clause_str] = sus_counter[clause_str] - 1

			del_result = mut_collection.delete_one({"_id": insert_id})

mut_collection.drop()

sus_clauses = []
for clause, count in sus_counter.items():
	if count > 0:
		sus_clauses.append(clause)

print("\nResult: ", sus_clauses)