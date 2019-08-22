import os
import unicodedata
from collections import defaultdict
import csv
from datetime import date
from biothings.utils.dataload import dict_sweep, open_anyfile
import requests
import json


# 加一層:clinial_validity
# clingen { clinical_validity:[
#								{......}
#										]}
def load_data(data_access):

	#current_time = date.today().strftime("-%Y-%m-%d")
	#file_name = "ClinGen-Gene-Disease-Summary{}.csv".format(str(current_time))
	file_name = "ClinGen-Gene-Disease-Summary-2019-08-05.csv"
	data_dir = os.path.join(data_access, file_name)

	# check if the file exist
	assert os.path.exists(data_dir), "input file '%s' does not exist" % data_dir

	with open_anyfile(data_dir) as input_file:

		for _ in range(4):
			next(input_file)

		header = next(input_file).strip().split(",")
		next(input_file)
		reader = csv.DictReader(set(list(input_file)), fieldnames = header, delimiter = ",")
		output = defaultdict(list)

		hgnc_dict = {}

		for row in reader:

			if not 'GENE ID (HGNC)' in row or not row['GENE ID (HGNC)']:
				continue

			hgnc_id = row['GENE ID (HGNC)'].split(':')[1]

			# retrieve ENTRNZ ID from mygene.info based on HGNC ID
			headers = {'content-type':'application/x-www-form-urlencoded'}
			params = 'q=hgnc_id&scopes=HGNC&fields=_id'
			res = requests.post('http://mygene.info/v3/query', data=params, headers=headers)
			json_data = json.loads(res.text)
			entrez_id = json_data[0]['_id']

			if hgnc_id not in hgnc_dict:
				hgnc_dict[hgnc_id] = entrez_id

			gene = {}
			gene['_id'] = entrez_id 
			gene['clingen'] = {}
			gene['clingen']['clinical_validity'] = {}
			key_list = ['DISEASE LABEL', 'DISEASE ID (MONDO)', 'SOP', 'CLASSIFICATION', 'ONLINE REPORT']

			for key in key_list:

				if key == 'DISEASE ID (MONDO)':
					old_key = key
					complete_key = 'mondo'

				else:
					old_key = key
					complete_key = key.lower().replace(' ', '_')

				gene['clingen']['clinical_validity'][complete_key] = row.get(old_key, None).lower()
				
			
			gene = dict_sweep(gene, vals = ['','null','N/A',None, [],{}])
			output[gene['_id']].append(gene)
			

		# merge duplicates, this amy happen when a gene causes multiple diseases amd has multiple labels
		for value in output.values():


			if len(value) == 1:
				yield value[0]

			else:
				yield {
					'_id':value[0]['_id'],
					'clingen': [v['clingen']['clinical_validity'] for v in value]
				}



if __name__ == '__main__':

	access = "/Users/chvbs2000/Desktop/biothing/rotation_project"
	load_data(access)
	
	







