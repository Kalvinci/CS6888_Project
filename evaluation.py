wrong_query = {"$and" : [{"year" : {"$gt" : 1900}}, {"$and" : [{"rated": "APPROVED"}, {"runtime" : {"$gt" : 50}}]}]}
correct_query = {"$and" : [{"year" : {"$gt" : 1900}}, {"$and" : [{"rated": "APPROVED"}, {"runtime" : {"$gt" : 60}}]}]}

wrong_query = {
	"$and": [
		{"cast": { "$all": ["Leonardo DiCaprio", "Kate Winslet"] }},
		{"countries": { "$all": ["USA"] }},
		{"genres": { "$in": ["Romance"] } },
		{"imdb.rating": { "$gt": 7 }},
		{"languages": { "$in": ["English"] }},
		{"year": { "$gte" :1990, "$lt": 2020 }},
		{"runtime": {"$gte": 110, "$lt": 200}},
		{"metacritic": {"$gte": 70}}
	]
}

correct_query = {
	"$and": [
		{"cast": { "$all": ["Leonardo DiCaprio", "Kate Winslet"] }},
		{"countries": { "$all": ["USA"] }},
		{"genres": { "$in": ["Romance"] } },
		{"imdb.rating": { "$gt": 7 }},
		{"languages": { "$in": ["English"] }},
		{"year": { "$gte" :1990, "$lt": 2020 }},
		{"runtime": {"$gte": 110, "$lt": 200}},
		{"metacritic": {"$gte": 60}}
	]
}

wrong_query = {
	"$and": [
		{"cast": { "$all": ["Leonardo DiCaprio", "Kate Winslet"] }},
		{"countries": { "$all": ["USA"] }},
		{"genres": { "$in": ["Romance"] } }
	],
	"$or": [
		{"imdb.rating": { "$gt": 7 }},
		{"languages": { "$in": ["English"] }},
		{"year": { "$gte" :1990, "$lt": 2020 }},
		{"runtime": {"$gte": 110, "$lt": 200}},
		{"metacritic": {"$gte": 70}}
	]
}