import dnf_converter

def parse(query):
	print("parsing the query...")
	query, clause_map, clause_assoc = dnf_converter.convert(query)
	cp_clause_list = []
	for cp in query["$or"]:
		clauses = []
		if "$and" in cp:
			for clause in cp["$and"]:
				clauses.append(clause)
		else:
			clause = cp
			clauses.append(clause)
		cp_clause_list.append({ "cp": cp, "clauses": clauses })
	return cp_clause_list, clause_map, clause_assoc