from threading import Thread
import unittest
import time

from testcontainers.postgres import PostgresContainer
import psycopg

class IsolationWithPostgresTestCase(unittest.TestCase):
    
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


    def test_without_dirty_read(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SHOW TRANSACTION ISOLATION LEVEL")
                default_transaction_isolation, = cur.fetchone()
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                cur.execute("SHOW TRANSACTION ISOLATION LEVEL")
                new_transaction_isolation, = cur.fetchone()
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("John Smith", 2500))
                id_employee, = cur.fetchone()
                
        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (3500, id_employee))
                    time.sleep(2)
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                global dirty_read_salary
                with conn.cursor() as cur:
                    time.sleep(1)
                    cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    dirty_read_salary, = cur.fetchone()
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()         

        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                new_salary, = cur.fetchone()            
        
        self.assertEqual(default_transaction_isolation, "read committed")
        self.assertEqual(new_transaction_isolation, "read uncommitted")
        self.assertEqual(dirty_read_salary, 2500)
        self.assertEqual(new_salary, 3500)

    def test_norepeatable_read_with_read_committed_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Beth Lee", 3500))
                id_employee, = cur.fetchone()

        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                global norepeatable_read_first_salary, norepeatable_read_second_salary
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    norepeatable_read_first_salary, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    norepeatable_read_second_salary, = cur.fetchone()

        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    time.sleep(1)
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (4500, id_employee))                  
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()         

        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                new_salary, = cur.fetchone()
                
        self.assertNotEqual(norepeatable_read_first_salary, norepeatable_read_second_salary)
        self.assertEqual(new_salary, 4500)

    def test_without_norepeatable_read_with_repeatable_read_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Dep Tunner", 3000))
                id_employee, = cur.fetchone()

        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                global without_norepeatable_read_first_salary, without_norepeatable_read_second_salary
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    without_norepeatable_read_first_salary, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    without_norepeatable_read_second_salary, = cur.fetchone()
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    time.sleep(1)
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (4000, id_employee))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()         

        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                new_salary, = cur.fetchone()
                
        self.assertEqual(without_norepeatable_read_first_salary, without_norepeatable_read_second_salary)
        self.assertEqual(without_norepeatable_read_first_salary, 3000)
        self.assertEqual(new_salary, 4000)

    def test_lost_update_with_read_committed_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Mary Castle", 4000))
                id_employee, = cur.fetchone()

        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
                    time.sleep(3)
                    new_salary = salary * 1.1
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, id_employee))
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
                    new_salary = salary * 1.2
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, id_employee))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
        
        self.assertEqual(salary, 4400)

    def test_without_lost_update_with_repeatable_read_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Bob Fox", 4000))
                id_employee, = cur.fetchone()

        def transaction1():
            global cm_lost_update
            with self.assertRaises(Exception) as cm_lost_update, psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
                    time.sleep(3)
                    new_salary = salary * 1.1
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, id_employee))
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
                    new_salary = salary * 1.2
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (new_salary, id_employee))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee,))
                    salary, = cur.fetchone()
        
        self.assertEqual(salary, 4800)
        self.assertEqual(type(cm_lost_update.exception), psycopg.errors.SerializationFailure)

    def test_read_skew_with_read_committed_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Amanda Lang", 4000))
                id_employee1, = cur.fetchone()
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("August Morse", 4000))
                id_employee2, = cur.fetchone()

        def transaction1():
            global read_skew_salary_employee1, read_skew_salary_employee2
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    read_skew_salary_employee1, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    read_skew_salary_employee2, = cur.fetchone()

        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    time.sleep(1)
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (5000, id_employee1))
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (5000, id_employee2))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    new_salary_employee1, = cur.fetchone()
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    new_salary_employee2, = cur.fetchone()
        
        self.assertEqual(read_skew_salary_employee1, 4000)
        self.assertEqual(read_skew_salary_employee2, 5000)
        
        self.assertEqual(new_salary_employee1, 5000)
        self.assertEqual(new_salary_employee2, 5000)

    def test_without_read_skew_with_repeatable_read_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Eric Wilson", 4000))
                id_employee1, = cur.fetchone()
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Roxy Clark", 4000))
                id_employee2, = cur.fetchone()

        def transaction1():
            global without_read_skew_salary_employee1, without_read_skew_salary_employee2
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    without_read_skew_salary_employee1, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    without_read_skew_salary_employee2, = cur.fetchone()

        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    time.sleep(1)
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (5000, id_employee1))
                    cur.execute("UPDATE employee SET salary=%s WHERE id=%s", (5000, id_employee2))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    new_salary_employee1, = cur.fetchone()
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    new_salary_employee2, = cur.fetchone()
        
        self.assertEqual(without_read_skew_salary_employee1, 4000)
        self.assertEqual(without_read_skew_salary_employee2, 4000)
        self.assertEqual(new_salary_employee1, 5000)
        self.assertEqual(new_salary_employee2, 5000)

    def test_write_skew_with_repeatable_read_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Selma Bates", 5000))
                id_employee1, = cur.fetchone()
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Samuel Bowen", 9000))
                id_employee2, = cur.fetchone()

        def transaction1():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT SUM(salary) FROM employee")
                    total_salary, = cur.fetchone()
                    increment_salary = 0.1 * total_salary
                    time.sleep(2)
                    cur.execute("UPDATE employee SET salary=salary+%s WHERE id=%s", (increment_salary, id_employee1))

        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    time.sleep(1)
                    cur.execute("SELECT SUM(salary) FROM employee")
                    total_salary, = cur.fetchone()
                    increment_salary = 0.1 * total_salary
                    cur.execute("UPDATE employee SET salary=salary+%s WHERE id=%s", (increment_salary, id_employee2))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    new_salary_employee1, = cur.fetchone()
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    new_salary_employee2, = cur.fetchone()
        
        self.assertEqual(new_salary_employee1, 6400)
        self.assertEqual(new_salary_employee2, 10400)

    def test_without_write_skew_with_serializable_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Dean Knox", 5000))
                id_employee1, = cur.fetchone()
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s) RETURNING id", ("Madison Frey", 9000))
                id_employee2, = cur.fetchone()

        def transaction1():
            global cm_write_skew
            with self.assertRaises(Exception) as cm_write_skew, psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    cur.execute("SELECT SUM(salary) FROM employee")
                    total_salary, = cur.fetchone()
                    increment_salary = 0.1 * total_salary
                    time.sleep(2)
                    cur.execute("UPDATE employee SET salary=salary+%s WHERE id=%s", (increment_salary, id_employee1))
                    
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                    time.sleep(1)
                    cur.execute("SELECT SUM(salary) FROM employee")
                    total_salary, = cur.fetchone()
                    increment_salary = 0.1 * total_salary
                    cur.execute("UPDATE employee SET salary=salary+%s WHERE id=%s", (increment_salary, id_employee2))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee1,))
                    new_salary_employee1, = cur.fetchone()
                    cur.execute("SELECT salary FROM employee WHERE id = %s", (id_employee2,))
                    new_salary_employee2, = cur.fetchone()

        self.assertEqual(type(cm_write_skew.exception), psycopg.errors.SerializationFailure)        
        self.assertEqual(new_salary_employee1, 5000)
        self.assertEqual(new_salary_employee2, 10400)

    def test_phantom_read_with_read_committed_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Alan Rock", 2500))
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Jess Tex", 3000))

        def transaction1():
            global phantom_read_first_sum_salary, phantom_read_second_sum_salary
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT SUM(salary) FROM employee")
                    phantom_read_first_sum_salary, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT SUM(salary) FROM employee")
                    phantom_read_second_sum_salary, = cur.fetchone()
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    time.sleep(1)
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Emma Crow", 3500))
        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT SUM(salary) FROM employee")
                    sum_salary,  = cur.fetchone()
        
        self.assertNotEqual(phantom_read_first_sum_salary, phantom_read_second_sum_salary)
        self.assertEqual(sum_salary, 9000)

    def test_without_phanton_read_with_repeatable_read_isolation_level(self):
        with psycopg.connect(self.connection_uri()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Rosie Cole", 2500))
                cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Iggy Bell", 3000))

        def transaction1():
            global without_phanton_read_first_sum_salary, without_phanton_read_second_sum_salary
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    cur.execute("SELECT SUM(salary) FROM employee")
                    without_phanton_read_first_sum_salary, = cur.fetchone()
                    time.sleep(2)
                    cur.execute("SELECT SUM(salary) FROM employee")
                    without_phanton_read_second_sum_salary, = cur.fetchone()
        
        def transaction2():
            with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    time.sleep(1)
                    cur.execute("INSERT INTO employee (name, salary) VALUES (%s, %s)", ("Hugo Cash", 3500))

        
        threads = [Thread(target=transaction1), Thread(target=transaction2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
        with psycopg.connect(self.connection_uri()) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT SUM(salary) FROM employee")
                    sum_salary,  = cur.fetchone()
        
        self.assertEqual(without_phanton_read_first_sum_salary, without_phanton_read_second_sum_salary)
        self.assertEqual(sum_salary, 9000)

