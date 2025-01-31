"""File that contains the code for the smallBank experiment"""
import json
import random
import csv
from importlib.resources import files
from typing import Optional, Any
from time import time
import psycopg2 as pg
from jsonschema import SchemaError, ValidationError, validate
from psycopg2 import sql
from cctest_core.protocol import Benchmark, TransactionResult

P_DEPOSIT_CHECKING = "depositChecking"
P_BALANCE = "balance"
P_TRANSACT_SAVINGS = "transactSavings"
P_AMALGAMATE = "amalgamate"
P_WRITE_CHECK = "writeCheck"

PROGRAMS = [
    P_DEPOSIT_CHECKING,
    P_BALANCE,
    P_TRANSACT_SAVINGS,
    P_AMALGAMATE,
    P_WRITE_CHECK
]

class SmallBank(Benchmark):
    """Class that contains the code for the smallBank experiment"""
    def __init__(self):
        self.url = None
        self.hotspot = []
        self.config_dict = {}

    def init_db(self, config_dict):
        self.config_dict = config_dict
        username = config_dict["dbUsername"]
        password = config_dict["dbPassword"]
        host = config_dict["dbUrl"]
        port = config_dict["dbPort"]
        database = config_dict["dbName"]
        self.url = "postgresql://" + str(username) + ":" + str(password) + "@" \
                    + str( host ) + ":" + str( port ) + "/" + str( database )
        conn = pg.connect(self.url)

        schema_path: str = str(files("cctest_core").joinpath("smallBank_schema.sql"))
        with open(schema_path, "r", encoding="utf-8") as schema:
            sql_schema = schema.read()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql_schema)
                    conn.commit()
                    print("[Smallbank] Database schema created")

                    for i in range(config_dict["smallBank"]["numberOfAccounts"]):
                        name = "name" + str(i+1)
                        cursor.execute(
                            sql.SQL("""INSERT INTO account (name, CustomerId)
                                    VALUES (%s, %s) 
                                    RETURNING CustomerID"""), (name, i)
                        )

                        customer_id = cursor.fetchone()
                        if customer_id is not None:
                            customer_id = customer_id[0]

                        cursor.execute(
                            sql.SQL("""INSERT INTO Checking (CustomerId, Balance)
                                    VALUES (%s, %s)"""), (customer_id, random.randint(100, 10000))
                        )

                        cursor.execute(
                            sql.SQL("""INSERT INTO Savings (CustomerId, Balance)
                                    VALUES (%s, %s)"""), (customer_id, random.randint(100, 10000))
                        )
                    conn.commit()
                    print("[Smallbank] Database instance created")

            except Exception as e:
                conn.rollback()
                raise e
            finally:
                conn.close()


    def _set_read_committed(self, conn):
        """Set the isolation level to read committed"""
        conn.set_session(isolation_level=pg.extensions.ISOLATION_LEVEL_READ_COMMITTED)


    def _set_snapshot_isolation(self, conn):
        conn.set_session(isolation_level=pg.extensions.ISOLATION_LEVEL_REPEATABLE_READ)


    def _set_serializable(self, conn):
        conn.set_session(isolation_level=pg.extensions.ISOLATION_LEVEL_SERIALIZABLE)


    def _set_isolation_level(self, conn, isolation_level):
        if isolation_level == "RC":
            self._set_read_committed(conn)
        elif isolation_level == "SI":
            self._set_snapshot_isolation(conn)
        elif isolation_level == "SSI":
            self._set_serializable(conn)
        else:
            raise ValueError(f"Unknown isolation level: {isolation_level}")

    def _sample_account(self, sb_config_dict: dict) -> int:
        """Sample an account number from the database by the given sampling method"""
        sampling_method = sb_config_dict["accountSamplingMethod"]
        num_accounts = sb_config_dict["numberOfAccounts"]
        if sampling_method == "zipfian":
            skew = sb_config_dict["zipfianSkew"]
            return self.zipfian(skew, num_accounts)
        if sampling_method == "hotspot":
            hotspot_size = sb_config_dict["hotspotSize"]
            hotspot_probability = sb_config_dict["hotspotProbability"]
            if random.random() < hotspot_probability:
                return random.randint(1, hotspot_size)
            return random.randint(hotspot_size + 1, num_accounts)
        raise ValueError(f"Unknown sampling method: {sampling_method}")

    def _sample_program(self, sb_config_dict: dict) -> str:
        weights = [
            sb_config_dict["programDepositCheckingSamplingWeight"],
            sb_config_dict["programBalanceSamplingWeight"],
            sb_config_dict["programTransactSavingsSamplingWeight"],
            sb_config_dict["programAmalgamateSamplingWeight"],
            sb_config_dict["programWriteCheckSamplingWeight"]
        ]

        # random selection based on weights
        return random.choices(PROGRAMS, weights, k=1)[0]

    def _get_program_isolation_level(self, sb_config_dict: dict, program: str) -> str:
        """Returns the configured isolation level for the given program"""
        if program == P_DEPOSIT_CHECKING:
            return sb_config_dict["programDepositCheckingAllocatedIsolationLevel"]
        if program == P_BALANCE:
            return sb_config_dict["programBalanceAllocatedIsolationLevel"]
        if program == P_TRANSACT_SAVINGS:
            return sb_config_dict["programTransactSavingsAllocatedIsolationLevel"]
        if program == P_AMALGAMATE:
            return sb_config_dict["programAmalgamateAllocatedIsolationLevel"]
        if program == P_WRITE_CHECK:
            return sb_config_dict["programWriteCheckAllocatedIsolationLevel"]
        raise ValueError(f"Unknown program: {program}")

    def _run_transact_once(self,
                           conn: pg.extensions.connection,
                           sb_config_dict: dict,
                           program: str,
                           accounts: tuple[int, int]) -> Optional[Any]:
        """
        Run the specified transaction, once.
        The return value is None if the transaction committed successfully,
        or the error object if aborted.
        """
        level = self._get_program_isolation_level(sb_config_dict, program)
        self._set_isolation_level(conn, level)
        if program == P_DEPOSIT_CHECKING:
            name = "name" + str(accounts[0])
            return self.deposit_checking(name, random.randint(1, 10), conn)
        if program == P_BALANCE:
            name = "name" + str(accounts[0])
            promotion = sb_config_dict["programBalanceIsPromoted"]
            assert promotion in ["false", "Savings", "Checking", "SavingsChecking"]
            return self.balance(name, promotion, conn)
        if program == P_TRANSACT_SAVINGS:
            name = "name" + str(accounts[0])
            return self.transact_savings(name, random.randint(1, 10), conn)
        if program == P_AMALGAMATE:
            name = "name" + str(accounts[0])
            name2 = "name" + str(accounts[1])
            return self.amalgamate(name, name2, conn)
        if program == P_WRITE_CHECK:
            name = "name" + str(accounts[0])
            promotion = sb_config_dict["programWriteCheckIsPromoted"]
            assert promotion in ["false", "Savings", "Checking", "SavingsChecking"]
            return self.write_check(name, random.randint(1, 10), promotion, conn)
        raise ValueError(f"Unknown program: {program}")

    def run_transact(self,
                     config_dict: dict,
                     conn: pg.extensions.connection,
                     process_id: Optional[int] = None,
                     logfile: Optional[str] = None) -> TransactionResult:
        """Runs a transaction in the benchmark based on the given config file"""
        sb_config_dict = config_dict["smallBank"]
        program = self._sample_program(sb_config_dict)
        level = self._get_program_isolation_level(sb_config_dict, program)
        account_1 = self._sample_account(sb_config_dict)
        account_2 = self._sample_account(sb_config_dict)
        while account_1 == account_2:
            account_2 = self._sample_account(sb_config_dict)
        accounts = (account_1, account_2)

        tres = TransactionResult(program, level)
        start = time()
        while(True):
            result = self._run_transact_once(conn, sb_config_dict, program, accounts)
            if result is not None:
                # an abort occurred
                if isinstance(result, pg.errors.DeadlockDetected): # pylint: disable=no-member
                    tres.num_deadlock_abort += 1
                elif isinstance(result, pg.errors.SerializationFailure) and "concurrent update" in str(result): # pylint: disable=no-member
                    tres.num_conc_abort += 1
                elif isinstance(result, pg.errors.SerializationFailure) and "pivot" in str(result): # pylint: disable=no-member
                    tres.num_serial_abort += 1
                else:
                    print(f"Unknown abort reason: {result}")
            else:
                # Successful commit
                break

        end = time()
        tres.total_time = end - start
        if logfile is not None:
            with open(logfile, "a", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["", tres.num_deadlock_abort, tres.num_conc_abort,
                                 tres.num_serial_abort, start, end, tres.total_time, process_id])

        return tres

    def check_config(self, config_dict):
        schema_path = str(files("cctest_core").joinpath("smallBank_config.json"))
        # print(schema_path)
        with open(schema_path, "r", encoding="utf-8") as schema:
            data = json.load(schema)
            try:
                validate(instance=config_dict, schema=data)
                print("Valid config file")
            except ValidationError as e:
                print(e)
                print("Invalid config file")
            except SchemaError as e:
                print(e)
                print("Invalid config file")


    def deposit_checking(self, name, value, conn):
        """Add the given value to the checking account"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name,)
            )

            customer_id = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("UPDATE Checking SET Balance = Balance + %s WHERE CustomerId = %s"),
                (value, customer_id)
            )

            conn.commit()
        except Exception as e:
            conn.rollback()
            return e


    def balance(self, name, promotion, conn):
        """Return the total balance of the checking and savings account"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name,)
            )

            customer_id = cursor.fetchone()[0]


            if "Savings" in promotion:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Savings WHERE CustomerId = %s FOR UPDATE"),
                    (customer_id,)
                )
            else:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Savings WHERE CustomerId = %s"),
                    (customer_id,)
                )

            balance = cursor.fetchone()[0]

            if "Checking" in promotion:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Checking WHERE CustomerId = %s FOR UPDATE"),
                    (customer_id,)
                )
            else:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Checking WHERE CustomerId = %s"),
                    (customer_id,)
                )

            balance += cursor.fetchone()[0]
            
            conn.commit()
        except Exception as e:
            conn.rollback()
            return e



    def transact_savings(self, name, value, conn):
        """Add the given value to the savings account"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name,)
            )

            customer_id = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("UPDATE Savings SET Balance = Balance + %s WHERE CustomerId = %s"),
                (value, customer_id)
            )

            conn.commit()
        except Exception as e:
            conn.rollback()
            return e


    def amalgamate(self, name1, name2, conn):
        """Move all the money from the savings account of name1 to the checking account of name2"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name1,)
            )

            customer_id1 = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name2,)
            )

            customer_id2 = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("""
                    UPDATE Savings as new 
                    SET Balance = 0
                    FROM Savings as old
                    WHERE new.CustomerId = %s 
                            AND old.CustomerId = new.CustomerId
                    RETURNING old.Balance """),
                    (customer_id1,)
            )

            balance1 = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("""
                    UPDATE Checking as new 
                    SET Balance = 0
                    FROM Checking as old
                    WHERE new.CustomerId = %s 
                            AND old.CustomerId = new.CustomerId
                    RETURNING old.Balance """),
                    (customer_id2,)
            )

            balance2 = cursor.fetchone()[0]

            cursor.execute(
                sql.SQL("""UPDATE Checking
                                SET Balance = Balance + %s + %s
                                WHERE CustomerId = %s"""),
                                (balance1, balance2, customer_id2)
            )

            conn.commit()
        except Exception as e:
            conn.rollback()
            return e


    def write_check(self, name, value, promotion, conn):
        """Write a check for the given value"""
        try:
            cursor = conn.cursor()
            cursor.execute(
                sql.SQL("SELECT CustomerId FROM Account WHERE Name = %s"),
                (name,)
            )

            customer_id = cursor.fetchone()[0]

            if "Savings" in promotion:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Savings WHERE CustomerId = %s FOR UPDATE"),
                    (customer_id,)
                )
            else:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Savings WHERE CustomerId = %s"),
                    (customer_id,)
                )

            balance_savings = cursor.fetchone()[0]


            if "Checking" in promotion:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Checking WHERE CustomerId = %s FOR UPDATE"),
                    (customer_id,)
                )
            else:
                cursor.execute(
                    sql.SQL("SELECT Balance FROM Checking WHERE CustomerId = %s"),
                    (customer_id,)
                )

            balance_checking = cursor.fetchone()[0]

            if balance_checking + balance_savings < value:
                cursor.execute(
                    sql.SQL("""UPDATE Checking
                                    SET Balance = Balance - (%s + 1)
                                    WHERE CustomerId = %s"""),
                                    (value, customer_id)
                )
            else:
                cursor.execute(
                    sql.SQL("""UPDATE Checking
                                    SET Balance = Balance - %s
                                    WHERE CustomerId = %s"""),
                                    (value, customer_id)
                )

            conn.commit()
        except Exception as e:
            conn.rollback()
            return e

    def check_consistency(self, config):
        # Note: The SmallBank benchmark does not provide constraints
        # that can be validated to detect concurrency anomalies.
        pass
