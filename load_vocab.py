import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from pathlib import Path
import datetime
from dotenv import dotenv_values

import os

def table_exists(cur, table_name):
    """Check if a table exists in the database."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = %s
        )
    """, (table_name,))
    return cur.fetchone()[0]

def empty_table():
	return env.get('DELETE_TABLES', True) in ['True', 'true', True, '1', 1]

def run_ddls(ddl_files):
	env = dotenv_values('.env')

	conn = psycopg2.connect(
		    dbname=connection_details["dbname"],
		    host=connection_details["server"],
		    user=connection_details["user"],
		    password=connection_details["password"],
		    port=connection_details["port"]
		)

	folder_path = './ddl'
	with conn.cursor() as cur:
		for file_name in ddl_files:
			file_content = ''
			with open(folder_path + '/' + file_name, mode='r') as f:
				print(file_name)
				for line in f:
					if env.get('CDM_SCHEMA') != '':
						file_content += line.replace('\"cdmDatabaseSchema\"', '"'+env.get("CDM_SCHEMA")+'"')
					else:
						file_content += line.replace('\"cdmDatabaseSchema\".', '')
				try:	
					cur.execute(file_content)
					conn.commit()
				except psycopg2.Error as e:
					print(f"Database error while processing {file_name}: {e}")
				except Exception as e:
					print(f"Error processing {file_name}. Error: {e}")

		conn.close()

def insert_statement_for(csv_filename, table_name, cols):
	query = f"""
	INSERT INTO {table_name}({cols})
	VALUES %s 
	"""

	if "concept.csv" in csv_filename.lower():
		query += "ON CONFLICT (concept_id) DO NOTHING;"
	elif "vocabulary.csv" in csv_filename.lower():
		query += "ON CONFLICT (vocabulary_id) DO NOTHING;"
	elif "concept_class.csv" in csv_filename.lower():
		query += "ON CONFLICT (concept_class_id) DO NOTHING;"
	elif "domain.csv" in csv_filename.lower():
		query += "ON CONFLICT (domain_id) DO NOTHING;"
	elif "relationship.csv" in csv_filename.lower():
		query += "ON CONFLICT (relationship_id) DO NOTHING;"
	elif "concept_relationship.csv" in csv_filename.lower():
		query += "ON CONFLICT (concept_id_1, concept_id_2, relationship_id) DO NOTHING;"
	elif "concept_synonym.csv" in csv_filename.lower():
		query += "ON CONFLICT (concept_id, concept_synonym_name) DO NOTHING;"
	elif "concept_ancestor.csv" in csv_filename.lower():
		query += "ON CONFLICT (ancestor_concept_id, descendant_concept_id, min_levels_of_separation, max_levels_of_separation) DO NOTHING;"
	elif "drug_strength.csv" in csv_filename.lower():
		query += "ON CONFLICT (drug_concept_id, ingredient_concept_id) DO NOTHING;"

	return query

def process_csv(csv, connection_details, cdm_schema, vocab_file_dir, chunk_size=1e6):
	print(f"Working on file {Path(vocab_file_dir) / csv}")
	start_time = datetime.datetime.now()
	print(f"Start time: {start_time}")

	total_lines = sum(1 for _ in open(Path(vocab_file_dir) / csv, 'r', encoding='utf-8'))
	print(f"Total lines: {total_lines}")
	processed_lines = 0

	try:
		# Connect to the database
		conn = psycopg2.connect(
		    dbname=connection_details["dbname"],
		    host=connection_details["server"],
		    user=connection_details["user"],
		    password=connection_details["password"],
		    port=connection_details["port"]
		)

		if cdm_schema != '':
			table_name = f"\"{cdm_schema}\".{csv.split('.')[0]}"
		else:
			table_name = f"{csv.split('.')[0]}"

		with conn.cursor() as cur:
			if table_exists(cur, table_name) and empty_table() == True:
				try:
					cur.execute(f"DELETE FROM {table_name};")
				except Exception as e:
					print(f"Error deleting data from {table_name}: {e}")

			# Use pandas read_csv with chunksize to process the CSV in chunks
			for chunk in pd.read_csv(
			    Path(vocab_file_dir) / csv, sep="\t", dtype=str, keep_default_na=False, na_values="", encoding='utf-8', chunksize=chunk_size
			):
				if csv.lower() in ["concept.csv", "concept_relationship.csv", "drug_strength.csv"]:
					chunk['valid_start_date'] = pd.to_datetime(chunk['valid_start_date'], format='%Y%m%d')
					chunk['valid_end_date'] = pd.to_datetime(chunk['valid_end_date'], format='%Y%m%d')

				if csv.lower() == "drug_strength.csv":
					columns_to_replace_na = [
					    "amount_value", "amount_unit_concept_id", "numerator_value", "numerator_unit_concept_id",
					    "denominator_value", "denominator_unit_concept_id", "box_size"
					]
					chunk[columns_to_replace_na] = chunk[columns_to_replace_na].fillna(0)

				chunk = chunk.fillna(np.nan).replace([np.nan], [None])

				tuples = [tuple(x) for x in chunk.to_numpy()]
				cols = ','.join(list(chunk.columns))

				query = insert_statement_for(csv, table_name, cols)
				psycopg2.extras.execute_values(cur, query, tuples, template=None, page_size=1000)

				processed_lines += len(chunk)
				print(f"Processed lines: {processed_lines}, Remaining lines: {total_lines - processed_lines}")

				conn.commit()

		conn.close()
		end_time = datetime.datetime.now()
		elapsed_time = end_time - start_time
		print(f"End time: {end_time}")
		print(f"Elapsed time: {elapsed_time}")
		print(f"Finished processing {csv}")

	except psycopg2.Error as e:
		print(f"Database error while processing {csv}: {e}")
	except Exception as e:
		print(f"Error processing {csv}. Error: {e}")


def load_vocab_from_csv(connection_details, cdm_schema, vocab_file_dir):
	# Explicit loading order to satisfy foreign keys:
	load_order = [
		"concept.csv",
		"vocabulary.csv",
		"concept_class.csv",
		"domain.csv",
		"relationship.csv",
		"concept_relationship.csv",
		"concept_synonym.csv",
		"concept_ancestor.csv",
		"drug_strength.csv"
	]

	for csv in load_order:
		csv_path = Path(vocab_file_dir) / csv
		if csv_path.exists():
			process_csv(csv, connection_details, cdm_schema, vocab_file_dir)
		else:
			print(f"{csv} not found in {vocab_file_dir}")


if __name__ == '__main__':

	# Load environment variables from .env file
	env = dotenv_values('.env')

	# Retrieve environment variables
	connection_details = {
		"server": env['SERVER'],
		"port": env['PORT'],
		"user": env['USERNAME'],
		"password": env['PASSWORD'],
		"dbname": env['DATABASE']
	}
	cdm_schema = env['CDM_SCHEMA']
	vocab_file_dir = env['VOCAB_FILE_DIR']

	if env.get('CREATE_TABLES', False) in ['True', 'true', True, '1', 1]:
		ddl_files = [
			'1_OMOPCDM_postgresql_5.4_ddl.sql', 
			'2_OMOPCDM_postgresql_5.4_serials.sql', 
			'3_OMOPCDM_postgresql_5.4_indices.sql', 
			'4_OMOPCDM_postgresql_5.4_primary_keys.sql',
		]
		run_ddls(ddl_files)

	load_vocab_from_csv(connection_details, cdm_schema, vocab_file_dir)

	if env.get('CREATE_TABLES', False) in ['True', 'true', True, '1', 1]:
		ddl_files = [
			'5_OMOPCDM_postgresql_5.4_constraints.sql',
			'6_Customizations.sql',
		]
		run_ddls(ddl_files)
