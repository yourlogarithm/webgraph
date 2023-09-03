import asyncio
import json
import os

import aiohttp
from aiokafka import AIOKafkaConsumer
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncManagedTransaction

from constants import DAMPING_FACTOR, MAX_ITERATIONS, AGGREGATOR_ENDPOINT, PROJECTION_GRAPH_NAME, NODE_NAME, CONNECTION_NAME

MAX_WORKERS = os.getenv('MAX_WORKERS', 10)


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
        async with driver.session(database='pages') as session:
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
    consumer = AIOKafkaConsumer('ranker', bootstrap_servers='localhost:9092')
    neo4j_credentials = os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password')
    neo4j_driver = AsyncGraphDatabase.driver('bolt://localhost:7687', auth=neo4j_credentials)
    client = aiohttp.ClientSession()
    await consumer.start()
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    print('Started...')
    try:
        async for msg in consumer:
            url, edges = json.loads(msg.value.decode('utf-8'))
            print(url)
            await task_queue.put(asyncio.create_task(insert_page(neo4j_driver, semaphore, url, edges)))
            async with client.get(AGGREGATOR_ENDPOINT) as response:
                if response.status == 200 and (await response.json())['calculate']:
                    await task_queue.put(asyncio.create_task(neo4j_driver.session(database='pages').write_transaction(pagerank)))
        await task_queue.join()
    finally:
        await consumer.stop()
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
