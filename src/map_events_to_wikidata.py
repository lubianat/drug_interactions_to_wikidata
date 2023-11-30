import pandas as pd
from pathlib import Path
from wdcuration import check_and_save_dict

from wdcuration.sheet_based_curation import BASIC_EXCLUSION

HERE = Path(__file__).parent.resolve()
DATA = HERE.parent.joinpath("data").resolve()
RESULTS = HERE.parent.joinpath("results").resolve()

df_events = pd.read_csv(RESULTS / "events.csv")

import json
with open(HERE.joinpath("dicts/events.json"), 'r') as json_file:
    events_dict = json.load(json_file, object_pairs_hook=lambda pairs: {str(k): v for k, v in pairs})


qs = ""

for k,v in events_dict.items():
    qs += f'{v}|P3201|"{k}"\n'

RESULTS.joinpath("events.qs").write_text(qs)

for i, row in df_events.iterrows():
            check_and_save_dict(
                master_dict={"events":events_dict},
                dict_name="events",
                string="",
                path= HERE/"dicts",
                dict_key=str(row["id"]),
                search_string=row["name"],
                format_function=str,
                excluded_types= [],
            )