import asyncio
import unittest
from decimal import Decimal
from qfex import QFEXConfig, QFEXTakerClient, Side

class EnqueueCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_queue_full_does_not_reserve_order_id(self):
        cfg = QFEXConfig(is_prod=False, symbol_list=['TEST'], public_key='', secret_key='', outbound_queue_max=1)
        client = QFEXTakerClient(cfg, strategy=None)
        client._trade_out_q.put_nowait({'occupied': True})
        with self.assertRaisesRegex(RuntimeError, 'queue full'):
            await client.send_ioc_limit('TEST', Side.BUY, Decimal('1'), Decimal('2'), 'retry-id')
        self.assertNotIn('retry-id', client._client_oid_to_fut)
        client._trade_out_q.get_nowait()
        task = asyncio.create_task(client.send_ioc_limit('TEST', Side.BUY, Decimal('1'), Decimal('2'), 'retry-id'))
        await asyncio.sleep(0)
        self.assertEqual(client._trade_out_q.get_nowait()['params']['client_order_id'], 'retry-id')
        await client._handle_order_response({'client_order_id':'retry-id', 'status':'CANCELLED'})
        self.assertEqual((await task)['final_order_response']['status'], 'CANCELLED')

    async def test_stopping_does_not_reserve_order_id(self):
        cfg = QFEXConfig(is_prod=False, symbol_list=['TEST'], public_key='', secret_key='')
        client = QFEXTakerClient(cfg, strategy=None)
        client.stop()
        with self.assertRaisesRegex(RuntimeError, 'stopping'):
            await client.send_ioc_limit('TEST', Side.BUY, Decimal('1'), Decimal('2'), 'stopped-id')
        self.assertNotIn('stopped-id', client._client_oid_to_fut)
