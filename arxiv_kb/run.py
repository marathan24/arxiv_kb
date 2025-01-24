import logging
import random
import arxiv
from typing import Dict, Any, List
from naptha_sdk.schemas import KBDeployment, KBRunInput
from naptha_sdk.storage.storage_provider import StorageProvider
from naptha_sdk.storage.schemas import CreateStorageRequest, ReadStorageRequest, DeleteStorageRequest, ListStorageRequest
from naptha_sdk.user import sign_consumer_id

from schemas import InputSchema, ScrapeArxivInput

logger = logging.getLogger(__name__)

class ArxivKB:
    def __init__(self, deployment: Dict[str, Any]):
        """Initialize the ArxivKB with deployment configuration"""
        self.deployment = deployment
        self.config = self.deployment.config
        self.storage_provider = StorageProvider(self.deployment.node)
        self.storage_type = self.config.storage_type
        self.table_name = self.config.path
        self.schema = self.config.schema
    
    async def init(self, *args, **kwargs):
        """Initialize the knowledge base by creating the table"""
        result = await create(self.deployment)
        return result

    def scrape_arxiv(self, query: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """
        Scrape arXiv for papers matching the query.
        Returns a list of dicts with 'title' and 'summary'.
        """
        try:
            search = arxiv.Search(
                query=query,
                max_results=max_results,
                sort_by=arxiv.SortCriterion.LastUpdatedDate
            )
            papers = []
            for result in search.results():
                papers.append({
                    "title": result.title,
                    "summary": result.summary
                })
                logger.info(f"Scraped paper: {result.title}")
            logger.info(f"Scraped {len(papers)} papers for query: {query}")
            return papers
        except Exception as e:
            logger.error(f"Error during arXiv scraping: {e}")
            return []
    
    async def scrape_and_add(self, input_data: Dict[str, Any], *args, **kwargs):
        """
        Scrape papers from arxiv and add them directly to the knowledge base.
        Uses the scrape_arxiv method to fetch papers and stores them in the database.
        """
        scrape_input = ScrapeArxivInput(**input_data)
        logger.info(f"Starting paper scraping with query: {scrape_input.query}")
        
        papers = self.scrape_arxiv(
            query=scrape_input.query,
            max_results=scrape_input.max_results
        )
        
        if not papers:
            return {
                "status": "error",
                "message": "No papers were scraped from arXiv"
            }

        papers_added = 0
        for paper in papers:
            paper_data = {
                "id": random.randint(1, 1000000),
                "title": paper["title"],
                "abstract": paper["summary"]
            }
            
            try:
                await self.storage_provider.execute(CreateStorageRequest(
                    storage_type=self.storage_type,
                    path=self.table_name,
                    data={"data": paper_data}
                ))
                papers_added += 1
            except Exception as e:
                logger.error(f"Error adding paper to database: {str(e)}")
                continue
            
        return {
            "status": "success", 
            "message": f"Successfully added {papers_added} papers to knowledge base"
        }

    async def run_query(self, input_data: Dict[str, Any], *args, **kwargs):
        """Query the knowledge base by paper title"""
        logger.info(f"Querying table {self.table_name} with query: {input_data['query']}")

        read_result = await self.storage_provider.execute(ReadStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"condition": {"title": input_data["query"]}}
        ))
        return {"status": "success", "message": f"Query results: {read_result}"}

    async def list_rows(self, input_data: Dict[str, Any], *args, **kwargs):
        """List rows from the knowledge base"""
        list_result = await self.storage_provider.execute(ListStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"limit": input_data.get('limit')}
        ))
        return {"status": "success", "message": f"List rows result: {list_result}"}

    async def delete_table(self, input_data: Dict[str, Any], *args, **kwargs):
        """Delete the entire knowledge base table"""
        delete_result = await self.storage_provider.execute(DeleteStorageRequest(
            storage_type=self.storage_type,
            path=input_data['table_name']
        ))
        return {"status": "success", "message": f"Delete table result: {delete_result}"}

    async def delete_row(self, input_data: Dict[str, Any], *args, **kwargs):
        """Delete a specific row from the knowledge base"""
        delete_result = await self.storage_provider.execute(DeleteStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"condition": input_data['condition']}
        ))
        return {"status": "success", "message": f"Delete row result: {delete_result}"}

async def create(deployment: KBDeployment):
    """
    Create the Arxiv Knowledge Base table
    """
    storage_provider = StorageProvider(deployment.node)
    storage_type = deployment.config.storage_type
    table_name = deployment.config.path
    schema = {"schema": deployment.config.schema}

    logger.info(f"Creating {storage_type} at {table_name} with schema {schema}")

    try:
        await storage_provider.execute(ListStorageRequest(
            storage_type=storage_type,
            path=table_name
        ))
        logger.info(f"Table {table_name} already exists")
        return {"status": "success", "message": f"Table {table_name} already exists"}
    except Exception:
        logger.info(f"Table {table_name} does not exist")

    await storage_provider.execute(CreateStorageRequest(
        storage_type=storage_type,
        path=table_name,
        data=schema
    ))

    logger.info(f"Successfully created table {table_name}")
    return {"status": "success", "message": f"Successfully created table {table_name}"}

async def run(module_run: Dict[str, Any], *args, **kwargs):
    """Run the Arxiv Knowledge Base deployment"""
    module_run = KBRunInput(**module_run)
    module_run.inputs = InputSchema(**module_run.inputs)
    arxiv_kb = ArxivKB(module_run.deployment)

    method = getattr(arxiv_kb, module_run.inputs.func_name, None)
    if not method:
        raise ValueError(f"Invalid function name: {module_run.inputs.func_name}")

    return await method(module_run.inputs.func_input_data)

if __name__ == "__main__":
    import asyncio
    import os
    from naptha_sdk.client.naptha import Naptha
    from naptha_sdk.configs import setup_module_deployment

    naptha = Naptha()

    deployment = asyncio.run(setup_module_deployment(
        "kb", 
        "arxiv_kb/configs/deployment.json",
        node_url=os.getenv("NODE_URL")
    ))

    inputs_dict = {
        "init": {
            "func_name": "init",
            "func_input_data": None,
        },
        "scrape_and_add": {
            "func_name": "scrape_and_add",
            "func_input_data": {
                "query": "quantum computing",
                "max_results": 5
            },
        },
        "run_query": {
            "func_name": "run_query",
            "func_input_data": {"query": "Quantum Supremacy"},
        },
        "list_rows": {
            "func_name": "list_rows",
            "func_input_data": {"limit": 3},
        },
        "delete_table": {
            "func_name": "delete_table",
            "func_input_data": {"table_name": "arxiv_kb"},
        },
        "delete_row": {
            "func_name": "delete_row",
            "func_input_data": {"condition": {"title": "Quantum Supremacy"}},
        },
    }

    module_run = {
        "inputs": inputs_dict["init"], # After initializing, change this to the desired function step by step. If deleted, use init again.
        "deployment": deployment,
        "consumer_id": naptha.user.id,
        "signature": sign_consumer_id(naptha.user.id, os.getenv("PRIVATE_KEY"))
    }

    response = asyncio.run(run(module_run))
    print("Response:", response)