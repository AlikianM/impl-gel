'''
cipapi_cancer_scrape_html_reports.py

Scrape GeL tables from all GMC cancer html reports.
Quick extraction of variants and metadata  

Created 11-09-2018 by Graham Rose
'''

import os
from bs4 import BeautifulSoup
import requests
import pandas as pd
from tabulate import tabulate

# IO paths
current_dir_path = os.path.dirname(os.path.realpath('__file__'))
html_dir = 'ir_html_files'
csv_dir_cache = 'ir_html_scraped_files/cache' # temp - remove after debug
csv_dir = 'ir_html_scraped_files'
report_dir_path = os.path.join(current_dir_path, html_dir)

# Setup pandas dataframes to hold all rows per table type (5 tables)
participant_info_df = pd.DataFrame()
tumour_info_df = pd.DataFrame({})
domain_1_variants_df = pd.DataFrame({})
domain_2_variants_df = pd.DataFrame({})
seq_quality_info_df = pd.DataFrame({})


# Loop all files within html output dir
for filename in os.listdir(report_dir_path):

    # Setup filename path
    filename_path = os.path.join(report_dir_path, filename)

    #If we have an html tag, then parse it (skipping supp. htmls currently - v1_8.supplementary.html)
    if filename_path.endswith('v1_8.html'):
        soup = BeautifulSoup(open(filename_path), 'html.parser')
        
        # Print html headers and participant id
        title = soup.title
        print("HTML title=", title.get_text(), sep="")

        participant_id = soup.find(attrs={"id": "participant_id"})
        print("{}{}".format("Participant ID=", participant_id.get_text()))

        # Known set of gel table titles within html file
        table_headers_dict = dict([
            ('Participant information', '_participant_info.csv'),
            ('Tumour information', '_tumour_info.csv'), 
            ('Domain 1 somatic variants', '_domain_1_variants.csv'),
            ('Domain 2 somatic variants', '_domain_2_variants.csv'),
            ('Sequencing quality information', '_seq_quality_info.csv')])

        # Find all table and h3 tags
        tables = soup.find_all("table")
        headers = soup.find_all("h3")

        # Create pandas dataframes from soup table tags
        df = pd.read_html(str(tables))

        # Number of tables found
        number_of_tables = len(df)
        table_counter = 0

        # Loop all tables found, look above tags for corresponding h3 tag (used as table title)
        while table_counter < number_of_tables:

            # Find first p tag above table
            prev_h3_tag = tables[table_counter].find_previous("h3")
            prev_h3_tag_value = prev_h3_tag.text
            
            # Match p tag with the dict of tables of interest
            # Output key values as filename suffix (else exceeds windows file length limits)
            for key in table_headers_dict:
                if key == prev_h3_tag_value:
                    filename_suffix = table_headers_dict[key] # suffix for csv

            # Print each table title to stdout
            print("table suffix=", filename_suffix, sep="")

            # Hold next table in dataframe
            df_position = df[table_counter]

            # Calc number of rows & cols in the table
            df_dimensions = df_position.shape
            row_number = df_dimensions[0]
            
            # Modify table to add pid as first column (and primary key for sql table)
            # Build a list of pids from html header that equals table row length, append to first row of all tables
            row_counter = 0
            row_list = []
            while row_counter < row_number:
                row_list.append(participant_id.get_text())
                row_counter += 1

            # Add new col with a header and pid value rows
            id_col = pd.Series(row_list, name="Participant ID")
            mod_table = pd.concat([id_col, df_position], axis=1, join='inner')

            # Build output path and write to csv each table
            file_output_path = os.path.join(
                current_dir_path, csv_dir_cache, (filename+filename_suffix))

            # Print to stdout and tables to file
            print("Dimensions=", df_dimensions)
            print("Output path=", file_output_path)
            print("Adding pid as first column...")
            print(tabulate(mod_table, headers='keys', tablefmt='psql'))
            #mod_table.to_csv(file_output_path, sep=',', index=False) # debug, no need to write to file

            # Write each table rows into concatenated export ready tables
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
                domain_2_variants_df = pd.concat([domain_2_variants_df, mod_table], sort=False)
            elif prev_h3_tag_value == "Sequencing quality information":
                seq_quality_info_df = pd.concat(
                    [seq_quality_info_df, mod_table], sort=False)

            table_counter += 1

# Print concatenated tables to stdout
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

# Build output path and write output csvs for each table
for key in table_headers_dict:
    filename = table_headers_dict[key]
    filename = filename.replace("_", "", 1) # tidy up name from dict

    file_output_path = os.path.join(
        current_dir_path, csv_dir, filename)
    
    participant_info_df.to_csv(file_output_path, sep=',', index=False)

# End
print("Done")
