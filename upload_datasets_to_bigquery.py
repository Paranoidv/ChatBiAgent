import pandas as pd
import bigquery_functions
import settings

# upload dataset
# df = pd.read_csv("datasets/bidataset/bi_first_quater_data.csv")

bigquery_functions.csv_to_bigquery(project_id=settings.project_id, 
                             dataset_id=settings.dataset_id, 
                             table_id="bidataset", 
                             csv_path="datasets/bidataset/bi_first_quater_data.csv", 
                             schema_path="datasets/bidataset/schema.json")

# upload dataset
# df = pd.read_csv("datasets/bidataset/bi_first_quater_data2.csv")

bigquery_functions.csv_to_bigquery(project_id=settings.project_id, 
                             dataset_id=settings.dataset_id, 
                             table_id="bidataset", 
                             csv_path="datasets/bidataset/bi_first_quater_data2.csv", 
                             schema_path="datasets/bidataset/schema.json")

