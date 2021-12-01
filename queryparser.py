import dnf_converter

# def parse(query):
# 	print("parsing the query...")
# 	query, clause_map = dnf_converter.convert(query)
# 	cp_clause_list = []
# 	for cp in query["$or"]:
# 		clauses = []
# 		if "$and" in cp:
# 			for clause in cp["$and"]:
# 				clauses.append(clause)
# 		else:
# 			clause = cp
# 			clauses.append(clause)
# 		cp_clause_list.append({ "cp": cp, "clauses": clauses })
# 	return cp_clause_list, clause_map

def parse(query):
	query, g_clause_map = dnf_converter.convert(query)
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
	return cp_clause_map, g_clause_map