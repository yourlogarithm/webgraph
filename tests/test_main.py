import unittest
from unittest.mock import Mock, call, AsyncMock, patch, ANY

from neo4j import AsyncGraphDatabase

import src.main as main
from src.constants import PROJECTION_GRAPH_NAME, NODE_NAME, CONNECTION_NAME


class TestMain(unittest.IsolatedAsyncioTestCase):
    async def test_drop_graph(self):
        mock_tx = AsyncMock()
        await main.drop_graph(mock_tx)
        mock_tx.run.assert_called_once_with('CALL gds.graph.drop(\'$projection_graph_name\')', projection_graph_name=PROJECTION_GRAPH_NAME)

    async def test_insert_page(self):
        driver = AsyncGraphDatabase.driver('bolt://localhost:7687', auth=("neo4j", "password"))
        mock_semaphore = AsyncMock()

        mock_url = 'http://example.com'
        mock_edges = ['http://example.com/1', 'http://example.com/2']

        with patch('src.main.insert_transaction', new_callable=AsyncMock) as mock_insert_transaction:
            # Neo4j uses those 2 fields internally, which breaks the test because they would return a mock objects - breaking the flow of the transaction
            mock_insert_transaction.metadata = None
            mock_insert_transaction.timeout = None
            await main.insert_page(driver, mock_semaphore, mock_url, mock_edges)

        mock_insert_transaction.assert_called_once_with(ANY, mock_url, mock_edges)

    async def test_pagerank(self):
        mock_tx = AsyncMock()
        await main.pagerank(mock_tx)
        mock_tx.run.assert_has_calls([
            call('CALL gds.graph.project(\'$projection_graph_name\', \'$node_name\', \'$connection_name\')',
                 projection_graph_name=PROJECTION_GRAPH_NAME, node_name=NODE_NAME, connection_name=CONNECTION_NAME),
            call(
                """
    CALL gds.pageRank.write(
        '$projection_graph_name',
        {
            maxIterations: $max_iterations,
            dampingFactor: $damping_factor,
            writeProperty: 'rank'
        }    
    )""",
                projection_graph_name=PROJECTION_GRAPH_NAME,
                max_iterations=main.MAX_ITERATIONS,
                damping_factor=main.DAMPING_FACTOR
            )
        ])
