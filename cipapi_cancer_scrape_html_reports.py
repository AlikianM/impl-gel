"""
cipapi_cancer_scrape_html_reports.py

Scrape GeL tables from all GMC cancer html reports.
Quick extraction of variants and metadata  

Created 11-09-2018 by Graham Rose
"""

import os
from bs4 import BeautifulSoup
import requests
import pandas as pd
from tabulate import tabulate

# IO paths setup
current_dir_path = os.path.dirname(os.path.realpath('__file__'))
html_dir = 'ir_html_files'
csv_dir_cache = 'ir_html_scraped_files/cache' # temp - remove after debug
csv_dir = 'ir_html_scraped_files'
report_dir_path = os.path.join(current_dir_path, html_dir)

# Pandas dataframes to hold all rows per table (5 tables currently scraped)
participant_info_df = pd.DataFrame()
tumour_info_df = pd.DataFrame({})
domain_1_variants_df = pd.DataFrame({})
domain_2_variants_df = pd.DataFrame({})
seq_quality_info_df = pd.DataFrame({})


"""
The html files are already present from the cipapi call. Here each file is
pushed through bs4 and the upto 5 tables present are output as csv for local
database acess and analysis. Uses the first previous h3 tag from each table tag
to identify the tables and name accordingly.
"""
for filename in os.listdir(report_dir_path):

    filename_path = os.path.join(report_dir_path, filename)

    # Currently skipping supp. htmls holding domain 3 data, which have the 
    # suffix of v1_8.supplementary.html

    if filename_path.endswith('v1_8.html'):
        soup = BeautifulSoup(open(filename_path), 'html.parser')
        
        # Some running stdout of progress

        title = soup.title
        print("HTML title=", title.get_text(), sep="")

        participant_id = soup.find(attrs={"id": "participant_id"})
        print("{}{}".format("Participant ID=", participant_id.get_text()))

        # These are the known set of gel table titles within each html file, 
        # and desired csv filenames for each as output.

        table_headers_dict = dict([
            ('Participant information', '_participant_info.csv'),
            ('Tumour information', '_tumour_info.csv'), 
            ('Domain 1 somatic variants', '_domain_1_variants.csv'),
            ('Domain 2 somatic variants', '_domain_2_variants.csv'),
            ('Sequencing quality information', '_seq_quality_info.csv')])

        tables = soup.find_all("table")
        headers = soup.find_all("h3")

        df = pd.read_html(str(tables))

        # Number of tables will vary dependant on the report content 
        # and variant found. Iterate based on this number.

        number_of_tables = len(df)
        table_counter = 0
        
        while table_counter < number_of_tables:
            prev_h3_tag = tables[table_counter].find_previous("h3")
            prev_h3_tag_value = prev_h3_tag.text
            
            # Match p tag with the dict of tables of interest
            # Output key values as filename suffix (else exceeds windows file length limits)

            for key in table_headers_dict:
                if key == prev_h3_tag_value:
                    filename_suffix = table_headers_dict[key] # suffix for csv

            # Hold next table in dataframe

            df_position = df[table_counter]

            # Calc number of rows and cols in the table, which
            # are needed to build the dataframe

            df_dimensions = df_position.shape
            row_number = df_dimensions[0]
            
            # Modify table to add pid as first column (and primary key for sql tables)
            # Build a list of pids from html header that equals table row length, append to first row of all tables

            row_counter = 0
            row_list = []
            while row_counter < row_number:
                row_list.append(participant_id.get_text())
                row_counter += 1

            # Add new col with a header and pid value rows

            id_col = pd.Series(row_list, name="Participant ID")
            mod_table = pd.concat([id_col, df_position], axis=1, join='inner')

            file_output_path = os.path.join(
                current_dir_path, csv_dir_cache, (filename+filename_suffix))

            print("Dimensions=", df_dimensions)
            print("Output path=", file_output_path)
            print("Adding pid as first column...")
            print(tabulate(mod_table, headers='keys', tablefmt='psql')) # debug use

            # Write each table rows into concatenated export ready dataframes
            if prev_h3_tag_value == "Participant information":
                participant_info_df = pd.concat(
                    [participant_info_df, mod_table], sort=False)
            elif prev_h3_tag_value == "Tumour information":
                tumour_info_df = pd.concat(
                    [tumour_info_df, mod_table], sort=False)
            elif prev_h3_tag_value == "Domain 1 somatic variants":
                domain_1_variants_df = pd.concat(
                    [domain_1_variants_df, mod_table], sort=False)
            elif prev_h3_tag_value == "Domain 2 somatic variants":
                domain_2_variants_df = pd.concat(
                    [domain_2_variants_df, mod_table], sort=False)
            elif prev_h3_tag_value == "Sequencing quality information":
                seq_quality_info_df = pd.concat(
                    [seq_quality_info_df, mod_table], sort=False)

            table_counter += 1

df_list = [participant_info_df, tumour_info_df, domain_1_variants_df, domain_2_variants_df, seq_quality_info_df]

# Print concatenated tables to stdout for progress

print("participant_info_df...",  tabulate(
    participant_info_df, headers='keys', tablefmt='psql'), sep='\n')
print("tumour_info_df...", tabulate(
    tumour_info_df, headers='keys', tablefmt='psql'), sep='\n')
print("domain_1_variants_df...", tabulate(
    domain_1_variants_df, headers='keys', tablefmt='psql'), sep='\n')
print("domain_2_variants_df...", tabulate(
    domain_2_variants_df, headers='keys', tablefmt='psql'), sep='\n')
print("seq_quality_info_df...", tabulate(
    seq_quality_info_df, headers='keys', tablefmt='psql'), sep='\n')

""" 
Build path and write each table as csv file
"""
table_counter = 0
for key in table_headers_dict:

    # filename output
    filename = table_headers_dict[key]
    filename = filename.replace("_", "", 1) # tidy up name from dict

    file_output_path = os.path.join(
        current_dir_path, csv_dir, filename)

    df_list[table_counter].to_csv(file_output_path, sep=',', index=False)

    table_counter += 1
    
print("Done")
