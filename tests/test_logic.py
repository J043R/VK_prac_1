"""Offline checks; these do NOT replace a real Lakehouse run."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from run import q18, canonical, Trino
import report


class Logic(unittest.TestCase):
    def test_q18_boundary_ties_and_multiple_orders_per_customer(self):
        db=sqlite3.connect(':memory:')
        db.executescript('''
          CREATE TABLE customer(custkey INTEGER,name TEXT);
          CREATE TABLE orders(orderkey INTEGER,custkey INTEGER,orderdate TEXT,totalprice NUMERIC);
          CREATE TABLE lineitem(orderkey INTEGER,linenumber INTEGER,quantity NUMERIC);
          INSERT INTO customer VALUES(1,'Customer#1'),(2,'Customer#2');
          INSERT INTO orders VALUES(10,1,'1995-01-01',100),(20,1,'1995-01-02',100),
            (30,2,'1995-01-01',999),(40,2,'1995-01-01',888),(50,2,'1995-01-01',100);
          INSERT INTO lineitem VALUES(10,1,150),(10,2,151),(20,1,200),(20,2,101),
            (30,1,150),(30,2,150),(40,1,299),(50,1,301);
        ''')
        answer=db.execute(q18('main')).fetchall()
        self.assertEqual([r[2] for r in answer],[10,50,20])
        self.assertEqual([r[4] for r in answer],[100,100,100])
        self.assertEqual([r[5] for r in answer],[301,301,301])
        # Independently materialize Silver and Gold, then compare to original Q18.
        db.executescript('''
          CREATE TABLE large_orders AS SELECT orderkey,sum(quantity) AS qty FROM lineitem
            GROUP BY orderkey HAVING sum(quantity)>300;
          CREATE TABLE silver AS SELECT trim(c.name) name,c.custkey,o.orderkey,o.orderdate,o.totalprice,l.quantity
            FROM customer c JOIN orders o ON c.custkey=o.custkey
            JOIN lineitem l ON o.orderkey=l.orderkey JOIN large_orders big ON o.orderkey=big.orderkey;
        ''')
        gold=db.execute('''SELECT name,custkey,orderkey,orderdate,totalprice,sum(quantity)
          FROM silver GROUP BY name,custkey,orderkey,orderdate,totalprice
          ORDER BY totalprice DESC,orderdate,orderkey LIMIT 100''').fetchall()
        self.assertEqual(canonical(answer),canonical(gold))

    def test_http_pagination_keeps_rows_and_session(self):
        responses=[{'id':'q1','columns':[{'name':'n'}],'data':[[1]],'nextUri':'http://trino/next'},
                   {'id':'q1','data':[[2]],'stats':{'state':'FINISHED'}}]
        with tempfile.TemporaryDirectory() as tmp, patch('run.request',side_effect=responses) as req:
            result=Trino(Path(tmp)).query('SELECT n',codec='GZIP')
            self.assertEqual(result['rows'],[[1],[2]])
            self.assertEqual(result['columns'],['n'])
            self.assertIn('lakekeeper.compression_codec=GZIP',req.call_args_list[0].args[2]['X-Trino-Session'])
            saved=list(Path(tmp).glob('*.json'))
            self.assertEqual(len(saved),1)
            self.assertEqual(json.loads(saved[0].read_text())['id'],'q1')

    def test_engine_errors_are_not_successful_measurements(self):
        with tempfile.TemporaryDirectory() as tmp, patch('run.request',return_value={'error':{'message':'bad SQL'}}):
            with self.assertRaisesRegex(RuntimeError,'bad SQL'):
                Trino(Path(tmp)).query('broken')
            self.assertTrue(list(Path(tmp).glob('error-*.json')))

    def test_report_does_not_claim_completion_without_measurements(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            with patch.object(report,'ROOT',root),patch.object(report,'R',root/'results'):
                report.generate()
            status=json.loads((root/'results/completion.json').read_text())
            self.assertFalse(status['complete'])
            text=(root/'отчет.md').read_text(encoding='utf-8')
            self.assertIn('не готова к сдаче',text)
            self.assertIn('не измерено',text)


if __name__=='__main__':
    unittest.main()
