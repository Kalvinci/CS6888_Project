import dnf_converter

def parse(query):
	query = dnf_converter.convert(query)
	cp_clause_map = {}
	cp_counter = 0
	for cp in query["$or"]:
		clause_map = {}
		clause_counter = 0
		for clause in cp["$and"]:
			clause_counter += 1
			clause_map["C"+str(clause_counter)] = clause
		cp_counter += 1
		cp_clause_map["CP"+str(cp_counter)] = { "CP": cp, "clauses": clause_map }
	return cp_clause_map