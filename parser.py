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
# use mygene.client to request url for id conversion (hgnc id -> entrenz id)

def load_data(data_access):

	current_time = date.today().strftime("-%Y-%m-%d")
	file_name = "ClinGen-Gene-Disease-Summary{}.csv".format(str(current_time))
	#file_name = "ClinGen-Gene-Disease-Summary-2019-08-05.csv"
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

		# initialize a list to store HGNC ID
		hgnc_list = []

		for row in reader:

			# skip samples with empty HGNC 
			if not 'GENE ID (HGNC)' in row or not row['GENE ID (HGNC)']:
				continue

			# store HGNC gen ID for conversion
			hgnc_id = row['GENE ID (HGNC)'].split(':')[1]
			hgnc_list.append(hgnc_id)

			# store every gene's information into a nested dictionary 
			gene = {}
			gene['_id'] = entrez_id 
			gene['clingen'] = {}
			gene['clingen']['clinical_validity'] = {}
			key_list = ['DISEASE LABEL', 'DISEASE ID (MONDO)', 'SOP', 'CLASSIFICATION', 'ONLINE REPORT']

			# for each key, store the value into the gene dictionary 
			for key in key_list:

				if key == 'DISEASE ID (MONDO)':
					old_key = key
					complete_key = 'mondo'

				else:
					old_key = key
					complete_key = key.lower().replace(' ', '_') #convert to lower case

				gene['clingen']['clinical_validity'][complete_key] = row.get(old_key, None).lower()
			
			gene = dict_sweep(gene, vals = ['','null','N/A',None, [],{}])
			output[gene['_id']].append(gene)

		entrenz_hgnc_dict = hgnc2entrenz(hgnc_list)

		# merge duplicates, this amy happen when a gene causes multiple diseases amd has multiple labels
		for value in output.values():

			final_output = {}

			if len(value) == 1:
				final_output.update(value[0])
				key = final_output['_id']
				final_output['_id'] = entrenz_hgnc_dict[key]
				yield final_output

				"""
				yield value[0]
				"""

			else:
				final_output.update({
					'_id':value[0]['_id'],
					'clingen': [v['clingen']['clinical_validity'] for v in value]
				})
				key = final_output['_id']
				final_output['_id'] = entrenz_hgnc_dict[key]
				yield final_output
				"""
				yield {
					'_id':value[0]['_id'],
					'clingen': [v['clingen'] for v in value]
				}
				"""

# convert HGNC ID to ENTRENZ GENE ID
def hgnc2entrenz(hgnc_list):

	"""
	output:
	dicionary[HGNC_ID] = Entrenz_ID

	"""

	hgnc_set = list(map(int, set(hgnc_list)))

	# retrieve ENTRNZ ID from mygene.info based on HGNC ID
	headers = {'content-type':'application/x-www-form-urlencoded'}
	params = 'q={}&scopes=HGNC&fields=_id'.format(str(hgnc_set).replace('[','').replace(']',''))
	res = requests.post('http://mygene.info/v3/query', data=params, headers=headers)
	json_data = json.loads(res.text)
	
	entrenz_dict = {}
	for i in range(len(json_data)):
		entrenz_dict[json_data[i]['query']] = json_data[i]['_id']

	return entrenz_dict


"""

if __name__ == '__main__':

	access = "/Users/chvbs2000/Desktop/biothing/rotation_project"
	load_data(access)
	
"""







