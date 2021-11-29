from sympy.logic.boolalg import to_dnf

operators = ["$or", "$and", "$nor"]

def convert_to_symbols(query, clause_counter = 0, clause_map = {}):
	q = ""
	for part in query:
		if part in operators:
			q += "("
			for p in query[part]:
				q_res, clause_counter, clause_map = convert_to_symbols(p, clause_counter, clause_map)
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
	print("\nconverting to intermediate form...")
	result_q, counter, clause_map = convert_to_symbols(query)
	print(result_q)
	intermediate_dnf = str(to_dnf(result_q))
	print("\nintermediate DNF -> ", intermediate_dnf)
	cps = [cp.strip() for cp in intermediate_dnf.split("|")]
	cp_queries = []
	for cp in cps:
		clauses = [clause.strip().strip("(").strip(")") for clause in cp.split("&")]
		clause_list = []
		for clause in clauses:
			if "~" in clause:
				clause_query = clause_map[clause[1:]]
				field = list(clause_query.keys())[0] 
				clause_query[field] = {"$not": {"$eq": clause_query[field]} if type(clause_query[field]) != dict else clause_query[field]}
				clause_list.append(clause_query)
			else:
				clause_list.append(clause_map[clause])
		if len(clause_list) > 1:
			cp_queries.append({"$and": clause_list})
		elif len(clause_list) == 1:
			cp_queries.append(clause_list[0])
	dnf_query = {"$or": cp_queries}
	print("\nDNF -> ", dnf_query)
	return dnf_query, clause_map