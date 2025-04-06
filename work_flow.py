from typing import Dict, TypedDict
from langgraph.graph import StateGraph, START, END
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain.chat_models import init_chat_model
from langchain_google_community import VertexAISearchRetriever
import prompts
import settings
from google.cloud import bigquery
import bigquery_functions
import utils
import json
import pandas as pd


class ChatBIAgentState(TypedDict):
    question: str
    database_schemas: str
    query: str
    max_num_retries: int
    num_retries_sql: int
    result_sql: str
    error_msg_sql: str
    df: pd.DataFrame
    visualize_request: str
    python_code_data_visualize: str
    python_code_store_variables_dict: dict
    num_retries_python_code_data_visualize: int
    result_python_code_data_visualize: str
    error_msg_python_code_data_visualize: str


llm = ChatGroq(model="llama3", temperature=0.3)
# llm = init_chat_model("deepseek-chat", model_provider="deepseek")
max_error_msg_num = 300

retriever = VertexAISearchRetriever(
    project_id=settings.project_id,
    location_id=settings.vertex_agent_builder_data_store_location,
    data_store_id=settings.vertex_agent_builder_data_store_id,
    max_documents=2,
    engine_data_type=1
)

def search_tables_and_schemas(state: ChatBIAgentState) -> ChatBIAgentState:
    # 解析输入
    docs_retrieved = retriever.invoke(state["question"])
    # eg. "找出一季度的XXX公司财务报表"
    tables_metadata = [json.loads(doc.page_content) for doc in docs_retrieved]

    schemas = []
    bq_client = bigquery.Client(project=settings.project_id)
    for table_metadata in tables_metadata:
        project_id = table_metadata["project_id"]
        dataset_id = table_metadata["dataset_id"]
        table_id = table_metadata["table_id"]
        schema = bigquery_functions.get_table_schema(bq_client, project_id, dataset_id, table_id)
        if schema:
            schemas.append(schema)
    
    state["database_schemas"] = "\n---------------\n".join(schemas)
    return state

def agent_sql_writer_node(state: ChatBIAgentState) -> ChatBIAgentState:

    prompt_template = ChatPromptTemplate(("system", prompts.system_agent_sql_promt))

    chain = prompt_template | llm

    response = chain.invoke({"question": state["question"], 
                             "database_schemas": state["database_schemas"]}).content
    state["query"] = utils.extract_code_block(content=response,language="sql")
    print(f" Agent SQL Query:\n {state['query']}")

    return state

def agent_sql_validator_node(state: ChatBIAgentState) -> ChatBIAgentState:
    bq_client = bigquery.Client(settings.project_id) 

    print("\n Agent SQL Query:")
    
    try:
        query = state["query"]
        job_config = bigquery.QueryJobConfig(dry_run=True)
        # SQL执行
        query_job = bq_client.query(query, job_config=job_config)
        
        # 保存dataframe
        df = bq_client.query(state["query"]).to_dataframe()
        state["df"] = df

        state["result_sql"] = "Pass"
        state["error_msg_sql"] = ""
        print(f"result: {state['result_sql']}")

        return state
        
    except Exception as e:
        state["num_retries_sql"] += 1

        state["result_sql"] = "Not Pass"
        state["error_msg_sql"] = str(e)[0:max_error_msg_num]
        print(f"result: {state['result_sql']}")
        print(f"error message: {state['error_msg_sql']}")

        print("\n Trying to fix the query: \n")
        prompt_template = ChatPromptTemplate(("system", prompts.system_agent_sql_validate_prompt))

        chain = prompt_template | llm


        response = chain.invoke({"query": state["query"], 
                                "error_msg": state["error_msg_sql"]}).content

        state["query"] = utils.extract_code_block(content=response,language="sql")
        print(f"\n Query adjusted:\n {state['query']}")

        return state

def agent_bi_expert_node(state: ChatBIAgentState) -> ChatBIAgentState:
    
    prompt_template = ChatPromptTemplate(("system", prompts.system_agent_bi_node_prompt))

    chain = prompt_template | llm

    response = chain.invoke({"question": state["question"],
                             "query": state["query"],
                             "df_structure": state["df"].dtypes,
                             "df_sample": state["df"].head(100)
                             }).content

    state["visualize_request"] = response
    print(f"\n Visualize Request:\n {state['visualize_request']}")

    return state

def agent_python_code_data_visualize_generator_node(state: ChatBIAgentState) -> ChatBIAgentState:

    prompt_template = ChatPromptTemplate(("system", prompts.system_agent_python_code_data_visualize_generate_node_prompt))

    chain = prompt_template | llm

    response = chain.invoke({"visualize_request": state["visualize_request"],
                             "df_structure": state["df"].dtypes,
                             "df_sample": state["df"].head(100)
                             }).content
    state["python_code_data_visualize"] = utils.extract_code_block(content=response,language="python")

    print(f"\n Data visualize code:\n {state['python_code_data_visualize']}")

    return state

def agent_python_code_data_visualize_validator_node(state: ChatBIAgentState) -> ChatBIAgentState:    

    print("\n Validating data visualize code:")
    
    try:
        df = state["df"]
        exec_globals = {"df": df}
        exec(state["python_code_data_visualize"], exec_globals)
        state["python_code_store_variables_dict"] = exec_globals
        state["result_python_code_data_visualize"] = "Pass"
        state["error_msg_python_code_data_visualize"] = ""
        print(f"result: {state['result_python_code_data_visualize']}")

        return state
    
    except Exception as e:
        state["num_retries_python_code_data_visualize"] += 1

        state["result_python_code_data_visualize"] = "Not Pass"
        state["error_msg_python_code_data_visualize"] = str(e)[0:max_error_msg_num]
        print(f"result: {state['result_python_code_data_visualize']}")
        print(f"error message: {state['error_msg_python_code_data_visualize']}")

        print("\n### Trying to fix the plotly code:")
        prompt_template = ChatPromptTemplate(("system", prompts.system_agent_python_code_data_visualize_generate_node_validate_prompt))

        chain = prompt_template | llm

        response = chain.invoke({"python_code_data_visualize": state["python_code_data_visualize"], 
                                "error_msg": state["error_msg_python_code_data_visualize"]}).content

        state["python_code_data_visualize"] = utils.extract_code_block(content=response,language="python")

        print(f"\n Plotly code adjusted:\n {state['python_code_data_visualize']}")

        return state

workflow = StateGraph(state_schema=ChatBIAgentState)

workflow.add_node("search_tables_and_schemas",search_tables_and_schemas)
workflow.add_node("agent_sql_writer_node",agent_sql_writer_node)
workflow.add_node("agent_sql_validator_node",agent_sql_validator_node)
workflow.add_node("agent_bi_expert_node",agent_bi_expert_node)
workflow.add_node("agent_python_code_data_visualize_generator_node",agent_python_code_data_visualize_generator_node)
workflow.add_node("agent_python_code_data_visualize_validator_node",agent_python_code_data_visualize_validator_node)

workflow.add_edge("search_tables_and_schemas","agent_sql_writer_node")
workflow.add_edge("agent_sql_writer_node","agent_sql_validator_node")

workflow.add_conditional_edges(
    'agent_sql_validator_node',
    lambda state: 'agent_bi_expert_node' 
    if state['result_sql']=="Pass" or state['num_retries_sql'] >= state['max_num_retries'] 
    else 'agent_sql_validator_node',
    {'agent_bi_expert_node': 'agent_bi_expert_node','agent_sql_validator_node': 'agent_sql_validator_node'}
)
workflow.add_edge("agent_bi_expert_node","agent_python_code_data_visualize_generator_node")
workflow.add_edge("agent_python_code_data_visualize_generator_node","agent_python_code_data_visualize_validator_node")

workflow.add_conditional_edges(
    'agent_python_code_data_visualize_validator_node',
    lambda state: "end" 
    if state['result_python_code_data_visualize']=="Pass" or state['num_retries_python_code_data_visualize'] >= state['max_num_retries'] 
    else 'agent_python_code_data_visualize_validator_node',
    {'end': END,'agent_python_code_data_visualize_validator_node': 'agent_python_code_data_visualize_validator_node'}
)

workflow.set_entry_point("search_tables_and_schemas")

app = workflow.compile()

### Run workflow
def run_workflow(question: str) -> dict:
    initial_state = ChatBIAgentState(
        question = question,
        database_schemas = "",
        query = "",
        num_retries_sql = 0,
        max_num_retries = 3,
        result_sql = "",
        error_msg_sql = "",
        df = pd.DataFrame(),
        visualization_request = "",
        python_code_data_visualize = "",
        python_code_store_variables_dict = {},
        num_retries_python_code_data_visualize = 0,
        result_python_code_data_visualize = "",
        error_msg_python_code_data_visualize = ""
    )
    final_state = app.invoke(initial_state)
    return final_state

# Test
# bi_flow = run_workflow(question = "一季度的XXX公司财务报表数据分析")