import os
import unicodedata
from collections import defaultdict
import csv
from datetime import date
from biothings.utils.dataload import dict_sweep, open_anyfile



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


		for row in reader:

			if not 'GENE ID (HGNC)' in row or not row['GENE ID (HGNC)']:
				continue

			gene = {}
			gene['_id'] = row['GENE ID (HGNC)'] 
			gene['clingen'] = {}
			key_list = ['GENE SYMBOL', 'DISEASE LABEL', 'DISEASE ID (MONDO)', 'SOP', 'CLASSIFICATION', 'ONLINE REPORT']
			#print (row)

			for key in key_list:

				old_key = key
				complete_key = key.lower().replace(' ', '_')
				gene['clingen'][complete_key] = row.get(old_key, None)
				
			
			gene = dict_sweep(gene, vals = ['','null','N/A',None, [],{}])
			output[gene['_id']].append(gene)
			

		# merge duplicates, this amy happen when a gene causes multiple diseases amd has multiple labels
		for value in output.values():

			if len(value) == 1:
				
				yield value[0]

			else:
				yield {
					'_id':value[0]['_id'],
					'clingen': [v['clingen'] for v in value]
				}
				
	

if __name__ == '__main__':

	access = "/Users/chvbs2000/Desktop/biothing/rotation_project"
	data = load_data(access)
	print (type(data))







