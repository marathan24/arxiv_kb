from pydantic import BaseModel
from typing import Literal, Optional, Dict, Any, List

class ScrapeArxivInput(BaseModel):
    query: str
    max_results: int = 10
    categories: Optional[List[str]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None

class InputSchema(BaseModel):
    func_name: Literal["init", "run_query", "add_data", "scrape_and_add", "delete_table", "delete_row", "list_rows"]
    func_input_data: Optional[Dict[str, Any]] = None