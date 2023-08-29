import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncManagedTransaction

MAX_WORKERS = os.getenv('MAX_WORKERS', 10)
RECALIBRATION_THRESHOLD = 10
RECALCULATION_THRESHOLD = 1000
MAX_ITERATIONS = 20
DAMPING_FACTOR = .85


async def insert_transaction(tx: AsyncManagedTransaction, url: str, edges: list[str]):
    await tx.run("""
    MERGE (page:Page {url: $url}) 
    ON CREATE SET page.rank = $rank 
    WITH page 
    UNWIND $edges AS edge 
    MERGE (other:Page {url: edge}) ON CREATE SET other.rank = $rank 
    MERGE (page)-[:LINKS_TO]->(other)""", url=url, edges=edges, rank=1 - DAMPING_FACTOR)


async def drop_graph(tx):
    await tx.run('CALL gds.graph.drop(\'connections\')')


async def recalibrate_transaction(tx: AsyncManagedTransaction, url: str):
    pass


async def insert_page(driver: AsyncDriver, semaphore: asyncio.Semaphore, url: str, edges: list[str], recalibrate: bool = False):
    async with semaphore:
        async with driver.session(database='pages') as session:
            await session.execute_write(insert_transaction, url, edges)
            if recalibrate:
                print('Recalibrating')
                await session.execute_write(recalibrate_transaction, url)
                print('Done recalibrating')


async def pagerank(tx: AsyncManagedTransaction):
    await tx.run("""
    CALL gds.graph.project(
        'connections',
        'Page',
        'LINKS_TO'
    )""")
    await tx.run("""
    CALL gds.pageRank.write(
        'connections',
        {
            maxIterations: $max_iterations,
            dampingFactor: $damping_factor,
            writeProperty: 'rank'
        }    
    )""", max_iterations=MAX_ITERATIONS, damping_factor=DAMPING_FACTOR)
    await drop_graph(tx)


async def main():
    consumer = AIOKafkaConsumer('ranker', bootstrap_servers='localhost:9092')
    neo4j_driver = AsyncGraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))
    await consumer.start()
    task_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    print('Started...')
    try:
        i = 0
        async for msg in consumer:
            url, edges = json.loads(msg.value.decode('utf-8'))
            print(url)
            await task_queue.put(asyncio.create_task(insert_page(neo4j_driver, semaphore, url, edges)))
            i += 1
            if i == RECALCULATION_THRESHOLD:
                i = 0
                print('Calculating pagerank')
                async with neo4j_driver.session(database='pages') as session:
                    await session.execute_write(pagerank)
                print('Done calculating pagerank')
        await task_queue.join()
    finally:
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(main())
