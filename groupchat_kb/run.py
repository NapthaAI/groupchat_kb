#!/usr/bin/env python
from dotenv import load_dotenv
from groupchat_kb.schemas import InputSchema
import random
from typing import Dict, Any
from naptha_sdk.schemas import KBRunInput, KBDeployment
from naptha_sdk.storage.schemas import CreateStorageRequest, ReadStorageRequest, ListStorageRequest, DeleteStorageRequest
from naptha_sdk.storage.storage_client import StorageClient
from naptha_sdk.user import sign_consumer_id
from naptha_sdk.utils import get_logger

load_dotenv()

logger = get_logger(__name__)

# You can create your module as a class or function
class GroupChatKB:
    def __init__(self, deployment: Dict[str, Any]):
        self.deployment = deployment
        self.config = self.deployment.config
        self.storage_client = StorageClient(self.deployment.node)
        self.storage_type = self.config.storage_config.storage_type
        self.table_name = self.config.storage_config.path
        self.schema = self.config.storage_config.storage_schema

    # TODO: Remove this. In future, the create function should be called by create_module in the same way that run is called by run_module
    async def init(self, *args, **kwargs):
        await create(self.deployment)
        return {"status": "success", "message": f"Successfully populated {self.table_name} table"}
    
    async def add_data(self, input_data: Dict[str, Any], *args, **kwargs):
        logger.info(f"Adding {(input_data)} to table {self.table_name}")

        # if row has no id, generate a random one
        if 'id' not in input_data:
            input_data['id'] = random.randint(1, 1000000)

        read_result = await self.storage_client.execute(ReadStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"conditions": [{"run_id": input_data["run_id"]}]}
        ))

        # make sure run_id are not in the table
        if len(read_result.data) > 0:
            return {"status": "error", "message": f"Run {input_data['run_id']} already exists in table {self.table_name}"}

        create_row_result = await self.storage_client.execute(CreateStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            data={"data": input_data}
        ))

        logger.info(f"Create row result: {create_row_result}")

        logger.info(f"Successfully added {input_data} to table {self.table_name}")
        return {"status": "success", "message": f"Successfully added {input_data} to table {self.table_name}"}

    async def list_rows(self, input_data: Dict[str, Any], *args, **kwargs):
        list_storage_request = ListStorageRequest(
            storage_type=self.storage_type,
            path=self.table_name,
            options={"limit": input_data['limit'] if input_data and 'limit' in input_data else None}
        )
        list_storage_result = await self.storage_client.execute(list_storage_request)
        logger.info(f"List rows result: {list_storage_result}")
        return {"status": "success", "message": f"List rows result: {list_storage_result}"}


    async def delete_table(self, input_data: Dict[str, Any], *args, **kwargs):
        delete_table_request = DeleteStorageRequest(
            storage_type=self.storage_type,
            path=input_data['table_name'],
        )
        delete_table_result = await self.storage_client.execute(delete_table_request)
        logger.info(f"Delete table result: {delete_table_result}")
        return {"status": "success", "message": f"Delete table result: {delete_table_result}"}

# TODO: Make it so that the create function is called when the kb/create endpoint is called
async def create(deployment: KBDeployment):
    """
    Create the Group Chat Knowledge Base table
    Args:
        deployment: Deployment configuration containing deployment details
    """

    storage_client = StorageClient(deployment.node)
    storage_type = deployment.config.storage_config.storage_type
    table_name = deployment.config.storage_config.path
    schema = {"schema": deployment.config.storage_config.storage_schema}

    logger.info(f"Creating {storage_type} at {table_name} with schema {schema}")

    create_table_request = CreateStorageRequest(
        storage_type=storage_type,
        path=table_name,
        data=schema
    )

    # Create a table
    create_table_result = await storage_client.execute(create_table_request)

    logger.info(f"Result: {create_table_result}")

# Default entrypoint when the module is executed
async def run(module_run: Dict):
    module_run = KBRunInput(**module_run)
    module_run.inputs = InputSchema(**module_run.inputs)
    groupchat_kb = GroupChatKB(module_run.deployment)
    method = getattr(groupchat_kb, module_run.inputs.func_name, None)
    return await method(module_run.inputs.func_input_data)

if __name__ == "__main__":
    import asyncio
    from naptha_sdk.client.naptha import Naptha
    from naptha_sdk.configs import setup_module_deployment
    import os

    naptha = Naptha()

    deployment = asyncio.run(setup_module_deployment("kb", "groupchat_kb/configs/deployment.json", node_url = os.getenv("NODE_URL")))

    inputs_dict = {
        "init": {
            "func_name": "init",
            "func_input_data": None,
        },
        "add_data": {
            "func_name": "add_data",
            "func_input_data": {
                "run_id": "123",
                "messages": [
                    {
                        "role": "user",
                        "content": "What is the capital of France?"
                    }
                ]
            },
        },
        "list_rows": {
            "func_name": "list_rows",
            "func_input_data": {"limit": 10},
        },
        "delete_table": {
            "func_name": "delete_table",
            "func_input_data": {"table_name": "groupchat_kb"},
        },
    }


    module_run = {
        "inputs": inputs_dict["delete_table"],
        "deployment": deployment,
        "consumer_id": naptha.user.id,
        "signature": sign_consumer_id(naptha.user.id, os.getenv("PRIVATE_KEY"))
    }

    response = asyncio.run(run(module_run))

    print("Response: ", response)