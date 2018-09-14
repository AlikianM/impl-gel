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
import re

# IO paths
current_dir_path = os.path.dirname(os.path.realpath('__file__'))
html_dir = 'ir_html_files'
csv_dir = 'ir_html_scraped_files'
report_dir_path = os.path.join(current_dir_path, html_dir)


# Cycle all files within html dir
for filename in os.listdir(report_dir_path):

    # Setup filename path
    filename_path = os.path.join(report_dir_path, filename)

    #If we have an html tag, then parse it (skipping supp. htmls currently - v1_8.supplementary.html)
    if filename_path.endswith('v1_8.html'):
        soup = BeautifulSoup(open(filename_path), 'html.parser')
        
        # Print html headers and participant id
        title = soup.title
        print("HTML title=", title.get_text())

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

        # Loop all tables found, look above for corresponding h3 tag (usedf as table title)
        while table_counter < number_of_tables:

            # Find first p tag above table
            prev_h3_tag = tables[table_counter].find_previous("h3")
            prev_h3_tag_value = prev_h3_tag.text
            
            # Match p tag with dict of known ones. 
            # Output corresponding suffix (else exceeds windows file length limits)
            for key in table_headers_dict:
                if key == prev_h3_tag_value:
                    filename_suffix = table_headers_dict[key] # used as suffix for csv filename

            # Print each found table to stdout
            #print("header=", filename_suffix)

            # Hold next table in dataframe
            df_position = df[table_counter]

            # Calc number of rows & cols in table
            df_dimensions = df_position.shape
            row_number = df_dimensions[0]
            
            # Build a list of participant id from html header that equals table row size, append to first row of all tables
            row_counter = 0
            row_list = []

            while row_counter < row_number:
                row_list.append(participant_id.get_text())
                row_counter += 1

            # New col values and header
            id_col = pd.Series(row_list, name="Participant ID")

            new_table = pd.concat([id_col, df_position], axis=1, join='inner')

            # Build output path and write to csv
            file_output_path = os.path.join(current_dir_path, csv_dir, (filename+filename_suffix))

            print("Dimensions=", df_dimensions)
            print("Output path=", file_output_path)
            print("Adding pid as first column")
            print(tabulate(new_table, headers='keys', tablefmt='psql'))

            # Write to separate csv files
            #new_table.to_csv(file_output_path, sep=',', index=False)

            # Write to single csv file per table type


            table_counter += 1



# End
print("Done")
