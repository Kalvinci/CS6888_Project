from pymongo import MongoClient
import math
import queryparser as qparser

# "mongodb+srv://kalyan:k4ly4nk4l1@cluster0.ersqz.mongodb.net/test"
client = MongoClient(port=27017)
db = client.testdb

primary_key = "Orderid"

oracle_map = {}
result_map = {}

allIds = set()
for document in db.order.find({}, {"Orderid": 1}):
	allIds.add(document["Orderid"])

wrong_query = {"$or": [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]}
correct_query = {"$or": [{"$and": [{"Year": {"$gt": 2009}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10007"}, {"Discount": 0}]}]}

for document in db.order.find(correct_query, {"_id": 0}):
	oracle_map[document[primary_key]] = document

for document in db.order.find(wrong_query, {"_id": 0}):
	result_map[document[primary_key]] = document


oracle_orderIds = set(oracle_map.keys())
result_orderIds = set(result_map.keys())

superflous = result_orderIds.difference(oracle_orderIds)
absent = oracle_orderIds.difference(result_orderIds)

parsed_query = qparser.parse(wrong_query)
cps = [item["cp"] for item in parsed_query.values()]
clauses = [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}, {"Zipcode": "10008"}, {"Discount": 0}]

totalPassIds = allIds.difference(superflous).difference(absent)
totalFailIds = allIds.difference(totalPassIds)

t_sus_score_map = {}
o_sus_score_map = {}

for clause in clauses:
	clausePassIds = set()
	for doc in db.order.find(clause):
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
	t_sus_score_map[str(clause)] = {"true": trueScore, "false": falseScore, "score": round((trueScore + falseScore), 2)}

for clause in clauses:
	clausePassIds = set()
	for doc in db.order.find(clause):
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
	o_sus_score_map[str(clause)] = {"true": trueScore, "false": falseScore, "score": round((trueScore + falseScore), 2)}

print("Tarantula:")
print(t_sus_score_map)
print("Ochiai:")
print(o_sus_score_map)