# from sympy import symbols
# from sympy.logic.boolalg import to_dnf

# c = []
# for i in range(5):
# 	c.append(symbols("c"+str(i)))

# print(to_dnf(c[0] & c[1]))

operators_l = ["$or", "$and", "$nor"]

def parse(query, clause_counter = 0, clause_map = {}):
	q = ""
	for part in query:
		if part in operators_l:
			q += "("
			for p in query[part]:
				q_res, clause_counter, clause_map = parse(p, clause_counter, clause_map)
				q += q_res
				last = len(query[part]) - 1
				if p != query[part][last]:
					if part == "$and":
						q += "&"
					elif part == "$or":
						q += "|"
		else:
			clauseId = "c"+str(clause_counter)
			clause_map[clauseId] = query
			clause_counter += 1
			q += clauseId
	if part in operators_l:
		q += ")"
	return q, clause_counter, clause_map

query = {"$or": [{"$and": [{"Year": {"$gt": 2007}}, {"Price": {"$gt": 100}}]}, {"$and": [{"Zipcode": "10008"}, {"Discount": 0}]}]}
result_q, counter, clause_map = parse(query)
print(result_q, clause_map)