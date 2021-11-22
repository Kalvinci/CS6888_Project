import dnf_converter

def parse(query):
	print("parsing the query...")
	query = dnf_converter.convert(query)
	cp_clause_list = []
	clause_list = []
	for cp in query["$or"]:
		clauses = []
		for clause in cp["$and"]:
			clauses.append(clause)
			clause_list.append(clause)
		cp_clause_list.append({ "cp": cp, "clauses": clauses })
	return cp_clause_list, clause_list