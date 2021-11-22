def getTruePositiveDocs(collection, cps, truePos, primary_key):
	replacement_docs = []
	for cp in cps:
		for doc in collection.find(cp):
			if doc[primary_key] in truePos:
				replacement_docs.append(doc)
				break
	return replacement_docs

def getTrueNegativeDoc(collection, clauses):
	query = {}
	for clause in clauses:
		for field in clause:
			query[field] = {"$not": {"$eq": clause[field]} if type(clause[field]) != dict else clause[field]}
	replacement_doc = None
	for doc in collection.find(query):
		replacement_doc = doc
		break
	return replacement_doc