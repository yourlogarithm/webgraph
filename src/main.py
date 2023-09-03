import asyncio
import json

import aiohttp
from aiokafka import AIOKafkaConsumer
from common_utils import persistent_execution
from common_utils.logger import Logger
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncManagedTransaction

from constants import DAMPING_FACTOR, MAX_ITERATIONS, AGGREGATOR_ENDPOINT, PROJECTION_GRAPH_NAME, NODE_NAME, CONNECTION_NAME
from settings import Settings


async def insert_transaction(tx: AsyncManagedTransaction, url: str, edges: list[str]):
    await tx.run("""
    MERGE (page:$node_name {url: $url}) 
    ON CREATE SET page.rank = $rank 
    WITH page 
    UNWIND $edges AS edge 
    MERGE (other:$node_name {url: edge}) ON CREATE SET other.rank = $rank 
    MERGE (page)-[:$connection_name]->(other)""", node_name=NODE_NAME, connection_name=CONNECTION_NAME, url=url, edges=edges, rank=1 - DAMPING_FACTOR)


async def drop_graph(tx):
    await tx.run('CALL gds.graph.drop(\'$projection_graph_name\')', projection_graph_name=PROJECTION_GRAPH_NAME,)


async def insert_page(driver: AsyncDriver, semaphore: asyncio.Semaphore, url: str, edges: list[str]):
    async with semaphore:
        async with driver.session() as session:
            await session.execute_write(insert_transaction, url, edges)


async def pagerank(tx: AsyncManagedTransaction):
    await tx.run("CALL gds.graph.project('$projection_graph_name', '$node_name', '$connection_name')",
                 projection_graph_name=PROJECTION_GRAPH_NAME, node_name=NODE_NAME, connection_name=CONNECTION_NAME)
    await tx.run("""
    CALL gds.pageRank.write(
        '$projection_graph_name',
        {
            maxIterations: $max_iterations,
            dampingFactor: $damping_factor,
            writeProperty: 'rank'
        }    
    )""", projection_graph_name=PROJECTION_GRAPH_NAME, max_iterations=MAX_ITERATIONS, damping_factor=DAMPING_FACTOR)
    await drop_graph(tx)


async def main():
    settings = Settings()
    consumer = AIOKafkaConsumer('ranker', bootstrap_servers=settings.kafka_uri)
    neo4j_driver = AsyncGraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    client = aiohttp.ClientSession()
    logger = Logger('webgraph', settings.log_level)
    await persistent_execution(consumer.start, tries=5, delay=5, backoff=5, logger_=logger)
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(settings.max_workers)
    logger.info('Started...')
    try:
        async for msg in consumer:
            url, edges = json.loads(msg.value.decode('utf-8'))
            logger.info(url)
            await task_queue.put(asyncio.create_task(insert_page(neo4j_driver, semaphore, url, edges)))
            async with client.get(AGGREGATOR_ENDPOINT) as response:
                if response.status == 200 and (await response.json())['calculate']:
                    await task_queue.put(asyncio.create_task(neo4j_driver.session().write_transaction(pagerank)))
        await task_queue.join()
    finally:
        await consumer.stop()
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
