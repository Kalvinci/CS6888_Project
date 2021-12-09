# NoSQL Fault Localization

## Requirements
- A local mongodb server running on port 27017

## Running the code
- To run the fault localizer
	`python fl.py [JSONFILE]`
- To run the baselines (Tarantual & Ochiai)
	`python baseline.py [JSONFILE]`
- Sample JSONFILE can be found under `/tests` folder

## Experiments
The experiments can be repeated by running `fl.py` and `baseline.py` on each of the test json file except `test0.json` under `/tests` folder.