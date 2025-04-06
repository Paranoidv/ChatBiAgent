import streamlit as st
import pandas as pd
import work_flow

# Page configuration
st.set_page_config(page_title="Chat BI Agent", layout="wide")

# Main application
st.title("📊 Chat BI Agent")

st.markdown("""
<style>
    .stDataFrame {width: 100% !important;}
    .stTextInput input {font-size: 16px;}
    .welcome-msg {color: #2e86c1; font-size: 1.1rem;}
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="welcome-msg">Chat BI Agent</p>', 
           unsafe_allow_html=True)

# Question input
question = st.text_input("Ask question:", 
                        placeholder="e.g. 一季度的XXX公司财务报表数据分析",
                        key="user_question")

if st.button("Run") and question:
    # Run the LangGraph workflow
    
    try:
        langraph_state = work_flow.run_workflow(question)
        
        column1, column2 = st.columns([0.3,0.7])

        with column1:
            st.subheader("Generated SQL Query")
            st.code(langraph_state.get("query", "No SQL generated"), language="sql")

        with column2:
            st.subheader("Result")
            fig = langraph_state.get("python_code_store_variables_dict").get("fig", None)
            string_viz_result = langraph_state.get("python_code_store_variables_dict").get("string_viz_result", None)
            df_viz = langraph_state.get("python_code_store_variables_dict").get("df_viz", None)
            if fig is None:
                if df_viz is not None:
                    st.table(df_viz)
                else:
                    st.markdown(string_viz_result)
            else:
                st.plotly_chart(fig)
                
    except Exception as e:
        st.error(f"Error processing your request: {str(e)}")

elif not question:
    st.warning("Please enter a question")
