from pymongo import MongoClient
import math
import queryparser as qparser
import sys, json
from tabulate import tabulate

def run(connection_url, db_name, collection_name, primary_key, test_query, oracle_query):
	client = MongoClient(connection_url)

	db = client[db_name]
	orig_collection = db[collection_name]
	primary_key = primary_key

	allIds = set()
	oracle_Ids = set()
	result_Ids = set()

	for document in orig_collection.find({}, {primary_key: 1}):
		allIds.add(document[primary_key])

	for doc in orig_collection.find(test_query, {primary_key: 1}):
		result_Ids.add(doc[primary_key])

	for doc in orig_collection.find(oracle_query, {primary_key: 1}):
		oracle_Ids.add(doc[primary_key])

	superflous = result_Ids.difference(oracle_Ids)
	absent = oracle_Ids.difference(result_Ids)

	cp_clause_list, g_clause_map, clause_assoc = qparser.parse(test_query)
	clause_list = list(g_clause_map.values())

	totalPassIds = allIds.difference(superflous).difference(absent)
	totalFailIds = allIds.difference(totalPassIds)

	t_sus_score_map = {}
	o_sus_score_map = {}

	for clause in clause_list:
		clausePassIds = set()
		for doc in orig_collection.find(clause):
			clausePassIds.add(doc[primary_key])
		clauseFailIds = allIds.difference(clausePassIds)
		tpratio = len(totalPassIds.intersection(clausePassIds)) / len(totalPassIds)
		tfratio = len(totalFailIds.intersection(clausePassIds)) / len(totalFailIds)
		denominator = tfratio + tpratio
		trueScore = 0 if denominator == 0 else round(tfratio / denominator, 2)
		fpratio = len(totalPassIds.intersection(clauseFailIds)) / len(totalPassIds)
		ffratio = len(totalFailIds.intersection(clauseFailIds)) / len(totalFailIds)
		denominator = ffratio + fpratio
		falseScore = 0 if denominator == 0 else round(ffratio / denominator, 2)
		t_sus_score_map[str(clause)] = round((trueScore + falseScore) / 2, 2)

	for clause in clause_list:
		clausePassIds = set()
		for doc in orig_collection.find(clause):
			clausePassIds.add(doc[primary_key])
		clauseFailIds = allIds.difference(clausePassIds)
		passed_c = len(totalPassIds.intersection(clausePassIds))
		failed_c = len(totalFailIds.intersection(clausePassIds))
		total_failed = len(totalFailIds)
		denominator = math.sqrt(total_failed * (failed_c + passed_c))
		trueScore = 0 if denominator == 0 else round(failed_c / denominator, 2)
		passed_c = len(totalPassIds.intersection(clauseFailIds))
		failed_c = len(totalFailIds.intersection(clauseFailIds))
		denominator = math.sqrt(total_failed * (failed_c + passed_c))
		falseScore =  0 if denominator == 0 else round(failed_c / denominator, 2)
		o_sus_score_map[str(clause)] = round((trueScore + falseScore) / 2, 2)

	print("\nTarantula:")
	print(tabulate(t_sus_score_map.items(), headers=["Clause", "Suspiciousness Score"], tablefmt="psql", showindex=False))
	print("\nOchiai:")
	print(tabulate(o_sus_score_map.items(), headers=["Clause", "Suspiciousness Score"], tablefmt="psql", showindex=False))


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