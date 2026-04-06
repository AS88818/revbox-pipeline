import os
import logging
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Ensure your OpenAI API key is in your environment variables
# os.environ["OPENAI_API_KEY"] = "your-key"

def map_unknown_column(unknown_header: str, sample_data: list) -> str:
    """
    Pings OpenAI to classify an unknown column header into our strict schema.
    """
    target_schema = [
        "policy_id", 
        "customer_name", 
        "policy_type", 
        "premium", 
        "effective_date", 
        "status", 
        "carrier_name",
        "ignore_column" # Fallback if it's junk data
    ]
    
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    
    prompt = PromptTemplate.from_template(
        """
        You are an expert data engineer mapping messy insurance data to a strict database schema.
        
        Unknown Column Header: '{unknown_header}'
        Sample Values from this column: {sample_data}
        
        Map this column to EXACTLY ONE of the following valid schema fields:
        {target_schema}
        
        If the column is irrelevant or junk data, output 'ignore_column'.
        Return ONLY the exact string of the mapped field. Do not add punctuation or explanation.
        """
    )
    
    chain = prompt | llm | StrOutputParser()
    
    try:
        logging.info(f"LLM analyzing unknown column: '{unknown_header}'...")
        result = chain.invoke({
            "unknown_header": unknown_header,
            "sample_data": sample_data,
            "target_schema": ", ".join(target_schema)
        })
        
        clean_result = result.strip().lower()
        
        if clean_result not in target_schema:
            logging.error(f"LLM hallucinated an invalid schema column: {clean_result}")
            return "ignore_column"
            
        logging.info(f"LLM successfully mapped '{unknown_header}' -> '{clean_result}'")
        return clean_result
        
    except Exception as e:
        logging.error(f"LLM mapping failed: {e}")
        return "ignore_column" # Defensive fallback so the pipeline doesn't crash