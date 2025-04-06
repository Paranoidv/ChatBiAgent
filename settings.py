from dotenv import load_dotenv
import os

project_id = "xxx"
dataset_id = "xxx"
vertex_agent_builder_data_store_location = "xxx"
vertex_agent_builder_data_store_id = "xxx"

load_dotenv("/home/local/path/.env")
os.environ["GROQ_API_KEY"] = os.getenv('GROQ_API_KEY')
