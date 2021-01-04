import logging
import configparser
import mysql.connector
from mysql.connector import Error
import csv
from csv import reader

logging.basicConfig(format='%(asctime)s :: %(levelname)s :: %(funcName)s :: %(lineno)d \
:: %(message)s', level = logging.INFO)

config = configparser.ConfigParser()
config.read('database.cfg')


class DataPipeline(object):

    def __init__(self):
        self.username=config['DATABASE']['DB_USER']
        self.password=config['DATABASE']['DB_PASSWORD']
        self.host=config['DATABASE']['DB_HOST']
        self.port=config['DATABASE']['DB_PORT']
        self.database=config['DATABASE']['DATABASE']

    def get_db_connection(self):
        """
         Function to get connection to MYSQL Db using the DB credentials passed via config file.

        """
        connection = None
        try:
            connection = mysql.connector.connect(user=self.username,
                                            password=self.password,
                                            host=self.host,
                                            port=self.port,
                                            database=self.database)
        except Exception as error:
            print("Error while connecting to database for job tracker", error)
        
        return connection
    
    def load_third_party(self,connection, file_path_csv):
        """
         The function to setup 'ticket_sales' table and load third-party CSV data

         Parameters:
               connection: MySQL DB connection object
               file_path_csv: Third-party CSV file path

        """
        cursor = connection.cursor()
        cursor.execute("""DROP TABLE IF EXISTS ticket_sales""")

        cursor.execute("""
            CREATE TABLE ticket_sales(
                    ticket_id INT,
                    trans_date DATE,
                    event_id INT,
                    event_name VARCHAR(50),
                    event_date DATE,
                    event_type VARCHAR(10),
                    event_city VARCHAR(20),
                    customer_id INT,
                    price DECIMAL(10,2),
                    num_tickets INT
                    );
           """
           )
        f = open(file_path_csv,"r")
        dt = reader(f)
        sql = """INSERT INTO ticket_sales VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        for row in dt:
            cursor.execute(sql, tuple(row))
        connection.commit()
        cursor.close()

    
    def query_popular_tickets(self,connection):
        """
        The function to get the most popular tickets

        Parameters:
               connection: DB connection object

        Returns: 
               records: DB record object

        """
        sql_statement = """
                WITH top_selling_tickets
                AS (SELECT 
                    event_name,
                    ROW_NUMBER() OVER (
                        ORDER BY num_tickets DESC) row_num
                    FROM 
                    ticket_sales
                )
                SELECT 
                   event_name
                FROM 
                top_selling_tickets
                WHERE row_num <= 3;"""
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        cursor.close()
        return records


if __name__ == "__main__":
    dp=DataPipeline()
    connection=dp.get_db_connection()
    dp.load_third_party(connection,"third_party_sales_1 - third_party_sales_1.csv")
    records=dp.query_popular_tickets(connection)
    print("Here are the most popular tickets in the past month: ")
    for rec in records:
        print("- ",rec[0])
    
