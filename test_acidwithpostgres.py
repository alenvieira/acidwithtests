from threading import Thread
import datetime
import unittest
import time

from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer
import psycopg

class AcidWithPostgresTestCase(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.postgres = PostgresContainer("postgres:16.2-alpine")
        cls.postgres.start()
        
        with psycopg.connect(cls.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE employee (id serial PRIMARY KEY, name text, salary double precision)")

    @classmethod
    def tearDownClass(cls):
        cls.postgres.stop()
    
    @classmethod
    def connection_uri(cls):
        return cls.postgres.get_connection_url().replace("+psycopg2", "")
    
    def tearDown(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM employee")

    def test_atomicity(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("John Smith", 2500))
        
        with self.assertRaises(Exception):
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Beth Lee", 3500))
                    raise RuntimeError
        
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, salary FROM employee")
                db_data = cur.fetchall()
        
        self.assertEqual(len(db_data), 1)
        self.assertEqual(db_data[0], ("John Smith", 2500))
    
    def test_consistency(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Dep Tunner", 3000))
        
        with self.assertRaises(Exception):
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Mary Castle", datetime.date(2024, 12, 20)))
    
        with self.assertRaises(Exception):
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Bob Fox", 2750))
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Alan Rock", datetime.date(2024, 12, 20)))
        
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, salary FROM employee")
                db_data = cur.fetchall()
        
        self.assertEqual(len(db_data), 1)
        self.assertEqual(db_data[0], ("Dep Tunner", 3000))
        
    def test_isolation(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Jess Tex", 4000))

        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id, salary FROM employee WHERE name = %s", ("Jess Tex",))
                    db_data = cur.fetchone()
                    time.sleep(4)
                    new_salary = db_data[1] * 1.1
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, db_data[0]))
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    time.sleep(2)
                    cur.execute("SELECT id, salary FROM employee WHERE name = %s", ("Jess Tex",))
                    db_data = cur.fetchone()
                    new_salary = db_data[1] * 1.2
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, db_data[0]))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE name = %s", ("Jess Tex",))
                    db_data = cur.fetchone()
        
        self.assertEqual(db_data[0], 4400)
    
    def test_durability(self):
        container = self.postgres.get_wrapped_container()
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Paul Port", 3000))
                conn.commit()
                container.stop()
                container.wait()

        with self.assertRaises(Exception) as cm:
            psycopg.connect(self.connection_uri())
        
        container.start()
        wait_for_logs(self.postgres, "LOG:  database system is ready to accept connections")

        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, salary FROM employee")
                db_data = cur.fetchall()
        
        self.assertEqual(type(cm.exception), psycopg.OperationalError)
        self.assertEqual(len(db_data), 1)
        self.assertEqual(db_data[0], ("Paul Port", 3000))
