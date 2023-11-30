import pandas as pd 
from pathlib import Path
from wdcuration import lookup_multiple_ids

HERE = Path(__file__).parent.resolve()
DATA = HERE.parent.joinpath('data').resolve()
RESULTS = HERE.parent.joinpath('results').resolve()

df= pd.read_excel(DATA /"nature2022_ddis.xlsx")

df = (df[['DRUG_1_CONCEPT_NAME', 'DRUG_2_CONCEPT_NAME', 'EVENT_CONCEPT_NAME',
       'MDR_CODE', 'MICROMEDEX_EVID_LEVEL']])

print(df.head())
df_mappings = pd.read_excel(DATA/"nature2022_drug_mappings.xlsx", dtype=str)
# From https://github.com/OHDSI/KnowledgeBase/blob/master/LAERTES/terminology-mappings/RxNorm-to-MeSH/mesh-to-rxnorm-standard-vocab-v5.csv
rxnorm_df = pd.read_csv(DATA/"mesh-to-rxnorm-standard-vocab-v5.csv", sep="|", dtype=str)

print(rxnorm_df)

# Remove any whitespace from column names
rxnorm_df.columns = rxnorm_df.columns.str.strip()

# Create a dictionary from the dataframe for mapping
rx_ext2rxnorm = pd.Series(rxnorm_df.concept_code.values,index=rxnorm_df.concept_id).to_dict()


# Read the sample data into dataframes
df_ddis = df

# Convert drug names in both dataframes to lowercase to enable case-insensitive matching
df_ddis['DRUG_1_CONCEPT_NAME'] = df_ddis['DRUG_1_CONCEPT_NAME'].str.lower()
df_ddis['DRUG_2_CONCEPT_NAME'] = df_ddis['DRUG_2_CONCEPT_NAME'].str.lower()
df_mappings['DRUG_CONCEPT_NAME'] = df_mappings['DRUG_CONCEPT_NAME'].str.lower()

df_mappings['RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)'] = df_mappings['RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)'].astype(str)

# Merge the dataframes to map the RXNORM codes to the drugs
df_ddis = df_ddis.merge(df_mappings[['DRUG_CONCEPT_NAME', 'RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)']], 
                        left_on='DRUG_1_CONCEPT_NAME', 
                        right_on='DRUG_CONCEPT_NAME', 
                        how='left')

# Rename the columns for clarity
df_ddis.rename(columns={'RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)': 'DRUG_1_CODE'}, inplace=True)

# Drop the now-redundant 'DRUG_CONCEPT_NAME' as we have matched the first drug
df_ddis.drop('DRUG_CONCEPT_NAME', axis=1, inplace=True)

# Repeat the process for the second drug
df_ddis = df_ddis.merge(df_mappings[['DRUG_CONCEPT_NAME', 'RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)']], 
                        left_on='DRUG_2_CONCEPT_NAME', 
                        right_on='DRUG_CONCEPT_NAME', 
                        how='left')

df_ddis.rename(columns={'RXNORM_CODE/RXNORM_EXTENSION_CODE (OHDSI)': 'DRUG_2_CODE'}, inplace=True)
df_ddis.drop('DRUG_CONCEPT_NAME', axis=1, inplace=True)
df_ddis.drop_duplicates(inplace=True)

df_ddis.to_csv(RESULTS/"clean_ddi.csv")

df_ddis['DRUG_1_CODE'] = df_ddis['DRUG_1_CODE'].str.lower()
df_ddis['DRUG_2_CODE'] = df_ddis['DRUG_2_CODE'].str.lower()

df_ddis['DRUG_1_CODE'] = df_ddis['DRUG_1_CODE'].map(rx_ext2rxnorm)
df_ddis['DRUG_2_CODE'] = df_ddis['DRUG_2_CODE'].map(rx_ext2rxnorm)

print(df_ddis)
# Get unique RXNORM codes for drugs and MDR codes for events
# Convert the numeric RXNORM and MDR codes to strings
unique_drug_codes = pd.concat([df_ddis['DRUG_1_CODE'], df_ddis['DRUG_2_CODE']]).unique().astype(str)
unique_event_codes = df_ddis['MDR_CODE'].unique().astype(str)
df_ddis['MDR_CODE'] = df_ddis['MDR_CODE'].astype(str)

print(unique_event_codes)
# Lookup the Wikidata QIDs for drugs and events
drug_qids = lookup_multiple_ids(list_of_ids=unique_drug_codes, wikidata_property='P3345')
event_qids = lookup_multiple_ids(list_of_ids=unique_event_codes, wikidata_property='P3201')

print(event_qids)
# Map the QIDs back to the dataframe
df_ddis['DRUG_1_QID'] = df_ddis['DRUG_1_CODE'].map(drug_qids)
df_ddis['DRUG_2_QID'] = df_ddis['DRUG_2_CODE'].map(drug_qids)
df_ddis['EVENT_QID'] = df_ddis['MDR_CODE'].map(event_qids)

print(df_ddis['MICROMEDEX_EVID_LEVEL'].unique())

circumstances_dict = {"Probable":"Q56644435", "Theoretical":"Q18603603", "Established":"Q123505415"}

df_ddis['SOURCING_QID'] = df_ddis['MICROMEDEX_EVID_LEVEL'].map(circumstances_dict)

# Save the updated dataframe
df_ddis.to_csv(RESULTS / "clean_ddi_with_wikidata.csv",index=False)


df_events = df_ddis[['MDR_CODE', 'EVENT_CONCEPT_NAME', 'EVENT_QID']].drop_duplicates()
df_events.columns = ["id", "name", "qid"]
df_events.to_csv(RESULTS / "events.csv",index=False)




qs = ""

for i,row in df_ddis.iterrows():
    qs += f"{row['DRUG_1_QID']}|P769|{row['DRUG_2_QID']}|S248|Q123478206|S1480|{row['SOURCING_QID']}"
    if row['EVENT_QID'] == row['EVENT_QID']:
        qs+= f"|P1909|{row['EVENT_QID']}\n"
    else: 
        qs+="\n"
    qs += f"{row['DRUG_2_QID']}|P769|{row['DRUG_1_QID']}|P1909|{row['EVENT_QID']}|S248|Q123478206|S1480|{row['SOURCING_QID']}"
    if row['EVENT_QID'] == row['EVENT_QID']:
        qs+= f"|P1909|{row['EVENT_QID']}\n"
    else: 
        qs+="\n"

RESULTS.joinpath("ddis_to_wikidata.qs").write_text(qs)