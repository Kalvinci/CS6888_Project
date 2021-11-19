from sympy.logic.boolalg import to_dnf

operators = ["$or", "$and", "$nor"]

def parse(query, clause_counter = 0, clause_map = {}):
	q = ""
	for part in query:
		if part in operators:
			q += "("
			for p in query[part]:
				q_res, clause_counter, clause_map = parse(p, clause_counter, clause_map)
				if part == "$nor":
					q_res = "~" + q_res
				q += q_res
				last = len(query[part]) - 1
				if p != query[part][last]:
					if part == "$and" or part == "$nor":
						q += "&"
					elif part == "$or":
						q += "|"
		else:
			clauseId = "c"+str(clause_counter)
			clause_map[clauseId] = query
			clause_counter += 1
			q += clauseId
	if part in operators:
		q += ")"
	return q, clause_counter, clause_map

def convert(query):
	result_q, counter, clause_map = parse(query)
	return to_dnf(result_q), clause_map

query = {"$or": [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]}
result, clause_map = convert(query)
print(result)