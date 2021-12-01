def getTruePositiveDocs(collection, cps):
	replacement_docs = []
	for cp in cps:
		for doc in collection.find(cp):
			replacement_docs.append(doc)
			break
	return replacement_docs

def getTrueNegativeDoc(collection, clauses):
	conditions = []
	for clause in clauses:
		for field in clause:
			conditions.append({field: {"$not": {"$eq": clause[field]} if type(clause[field]) != dict else clause[field]}})
	replacement_doc = None
	query = {"$and": conditions}
	for doc in collection.find(query):
		replacement_doc = doc
		break
	return replacement_doc