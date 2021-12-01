import queryparser as qparser
from replacement import getTruePositiveDocs, getTrueNegativeDoc
from pymongo import MongoClient
import sys, json
from tabulate import tabulate

def run(connection_url, db_name, collection_name, primary_key, test_query, oracle_query):
	client = MongoClient(connection_url)

	db = client[db_name]
	orig_collection = db[collection_name]
	primary_key = primary_key

	oracle_Ids = set()
	result_Ids = set()

	for doc in orig_collection.find(test_query, {primary_key: 1}):
		result_Ids.add(doc[primary_key])

	for doc in orig_collection.find(oracle_query, {primary_key: 1}):
		oracle_Ids.add(doc[primary_key])

	superflous = result_Ids.difference(oracle_Ids)
	absent = oracle_Ids.difference(result_Ids)

	cp_clause_list, g_clause_map, clause_assoc = qparser.parse(test_query, True)
	correct_cp_clause_list, correct_g_clause_map, correct_clause_assoc = qparser.parse(oracle_query)

	correct_cps = [item["cp"] for item in correct_cp_clause_list]
	correct_clause_list = list(correct_g_clause_map.values())

	sus_counter = {}

	superflous_clausemap = {}
	absent_clausemap = {}

	print("\nComputing Suspiciousness counters...")
	for item in cp_clause_list:
		pass_cp_ids = set()
		cp = item["cp"]
		clauses = item["clauses"]

		for document in orig_collection.find({"$and": [cp, {primary_key: {"$in": list(superflous)}}]}):
			pass_cp_ids.add(document[primary_key])
		
		for id in pass_cp_ids:
			for clause in clauses:
				field = list(clause.keys())[0]
				clause_str = str(clause)
				
				if id in superflous_clausemap:
					superflous_clausemap[id].append({"field": field, "clause_str": clause_str, "clause": clause})
				else:
					superflous_clausemap[id] = [{"field": field, "clause_str": clause_str, "clause": clause}]

				if clause_str in sus_counter:
					sus_counter[clause_str] += 1
				else:
					sus_counter[clause_str] = 1

		fail_cp_ids = absent.difference(pass_cp_ids)
		for clause in clauses:
			field = list(clause.keys())[0]
			clause_str = str(clause)
			pass_clause_ids = set()
			
			for doc in orig_collection.find({"$and": [clause, {primary_key: {"$in": list(absent)}}]}):
				pass_clause_ids.add(doc[primary_key])
			
			fail_clause_ids = fail_cp_ids.difference(pass_clause_ids)
			for id in fail_clause_ids:
				if id in absent_clausemap:
					absent_clausemap[id].append({"field": field, "clause_str": clause_str, "clause": clause})
				else:
					absent_clausemap[id] = [{"field": field, "clause_str": clause_str, "clause": clause}]

				if clause_str in sus_counter:
					sus_counter[clause_str] += 1
				else:
					sus_counter[clause_str] = 1

	print(tabulate(sus_counter.items(), headers=["Clause", "Suspiciousness Counter"], tablefmt="psql", showindex=False))

	print("\nExonerating innocent clauses...")

	local_client = MongoClient(port=27017)
	local_db = local_client["testdb"]
	mut_collection = local_db["mutation"]

	replacement_docs = getTruePositiveDocs(orig_collection, correct_cps)

	for doc in orig_collection.find({primary_key: {"$in": list(superflous)}}):
		for r_doc in replacement_docs:
			id = doc[primary_key]
			for c_map in superflous_clausemap[id]:
				field = c_map["field"]
				clause_str = c_map["clause_str"]
				
				mutant_doc = {}
				for key in doc:
					if key == "_id":
						continue
					if key == field:
						mutant_doc[key] = r_doc[field] if field in r_doc else None
					else:
						mutant_doc[key] = doc[key]
				
				insert_result = mut_collection.insert_one(mutant_doc)
				insert_id = insert_result.inserted_id

				ret = set()
				for res_row in mut_collection.find(oracle_query):
					ret.add(res_row["_id"])

				if len(ret) > 0:
					for c_str in clause_assoc[clause_str]:
						if c_str in sus_counter:
							sus_counter[c_str] -= 1
				
				del_result = mut_collection.delete_one({"_id": insert_id})

	replacement_doc = getTrueNegativeDoc(orig_collection, correct_clause_list)

	if replacement_doc != None:
		for doc in orig_collection.find({primary_key: {"$in": list(absent)}}):
			id = doc[primary_key]
			for c_map in absent_clausemap[id]:
				field = c_map["field"]
				clause_str = c_map["clause_str"]
				
				mutant_doc = {}
				for key in doc:
					if key == "_id":
						continue
					if key == field:
						mutant_doc[key] = replacement_doc[field] if field in replacement_doc else None
					else:
						mutant_doc[key] = doc[key]

				insert_result = mut_collection.insert_one(mutant_doc)
				insert_id = insert_result.inserted_id
				
				ret = set()
				for res_row in mut_collection.find(oracle_query):
					ret.add(res_row["_id"])

				if len(ret) > 0:
					sus_counter[clause_str] -= 1

				del_result = mut_collection.delete_one({"_id": insert_id})

	mut_collection.drop()

	sus_clauses = []
	for clause, count in sus_counter.items():
		if count > 0:
			sus_clauses.append(clause)
	
	print("\nResult:")
	print(tabulate([[s_c] for s_c in sus_clauses], headers=["Suspicious Clauses"], tablefmt="psql", showindex=False))

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print("Insufficient arguments")
	else:
		file_name = sys.argv[1]
		file = open(file_name)
		input_data = json.load(file)
		connection_url = input_data["connection_url"]
		db_name = input_data["db_name"]
		collection_name = input_data["collection_name"]
		primary_key = input_data["primary_key"]
		test_query = input_data["test_query"]
		oracle_query = input_data["oracle_query"]
		run(connection_url, db_name, collection_name, primary_key, test_query, oracle_query)