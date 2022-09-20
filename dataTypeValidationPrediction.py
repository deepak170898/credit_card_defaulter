import csv
import os
import sqlite3
import shutil
from this_app_logger.logger import App_Logger


class DbOperation:
    """this class is to be used to handle all SQL operations"""

    def __init__(self):
        self.path = 'predictionDatabase/'
        self.bad_file_path = "predictionRawFilesValidated/badRowData"
        self.good_file_path = "predictionRawFilesValidated/goodRawData"
        self.logger = App_Logger()
        if not os.path.isdir("predictionRawFilesValidated/badRowData"):
            os.makedirs("predictionRawFilesValidated/badRowData")
        if not os.path.isdir("predictionRawFilesValidated/goodRawData"):
            os.makedirs("predictionRawFilesValidated/goodRawData")

    def database_connection(self, database_name):
        """method name: database_connection
           description: this creates the database with the given name and if database already exists then opens the
           connection to the DB
           output: connection to the database
        """
        if not os.path.isdir("predictionLogs"):
            os.makedirs("predictionLogs")

        try:
            conn = sqlite3.connect(self.path + database_name + '.db')
            file = open("predictionLogs/databaseConnectingLogs.txt", "a+")
            self.logger.log(file, "opened %s database successfully" % database_name)
            file.close()
        except ConnectionError:
            file = open("predictionLogs/database_connectionLogs.txt", "a+")
            self.logger.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def create_table_db(self, database_name, column_names):
        """method name: create_table_db \n
           description: this method creates a table in the given database which will be
           used to insert good data after the raw data validation is done """
        try:
            conn = self.database_connection(database_name)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type ='table' AND name = 'goodRawData' ")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("training_logs/dbTableCreateLogs.txt", 'a+')
                self.logger.log(file, "Tables created successfully.")
                file.close()

                file = open("training_logs/database_connectionLogs.txt", "a+")
                self.logger.log(file, "Closed %s database successfully" % database_name)
                file.close()

            else:

                for key in column_names.keys():
                    type_ = column_names[key]

                    try:

                        conn.execute('ALTER TABLE goodRawData ADD COlUMN "{column_name}" '
                                     '{data_type}'.format(column_name=key, data_type=type_))

                    except:

                        conn.execute('CREATE TABLE  goodRawData ({column_name} {data_type})'.format(column_name=key,
                                                                                                    data_type=type_))

            conn.close()

            file = open("predictionLogs/dbTableCreateLogs.txt", 'a+')
            self.logger.log(file, "tables created successfully")
            file.close()

            file = open("predictionLogs/database_connectionLogs.txt", 'a+')
            self.logger.log(file, "closed %s database successfully" % database_name)
            file.close()

        except Exception as e:
            file = open("predictionLogs/db_table_create_logs.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("predictionLogs/database_connectionLogs.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % database_name)
            file.close()
            raise e

    def insert_into_table_good_data(self, database_name):
        """method name: insert_into_table_good_data
        description: this method will insert good data files from good data folder into the above
        created table """

        conn = self.database_connection(database_name)
        good_file_path = self.good_file_path
        bad_file_path = self.bad_file_path

        only_files = [f for f in os.listdir(good_file_path)]
        log_file = open("predictionLogs/dbInsertionLogs.txt", "a+")

        for file in only_files:
            try:
                with open(good_file_path + "/" + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO goodRawData values ({values})'.format(values=list_))

                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file, "Error while creating table:%s" % e)
                shutil.move(good_file_path + '/' + file, bad_file_path)
                self.logger.log(log_file, "file moved successfully %s" % file)
                log_file.close()
                raise e

        conn.close()
        log_file.close()

    def selecting_data_from_table_into_csv(self, database_name):
        """method name: selecting_data_from_table_into_csv
           description: exports data in goodDataTable as CSV file in a given location
           """
        self.file_from_db = 'predictionFileFromDb/'
        self.file_name = "inputFile.csv"
        log_file = open("predictionLogs/exportToCSV", "a+")

        try:
            conn = self.database_connection(database_name)

            sql_select = "SELECT * FROM goodRawData"

            cursor = conn.cursor()
            cursor.execute(sql_select)
            results = cursor.fetchall()

            headers = [i[0] for i in cursor.description]
            # to get the header of the csv file
            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)
            # open csv file for writing

            csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline=''), lineterminator='\r\n',
                                  quoting=csv.QUOTE_ALL, escapechar='\\')

            # add the headers and data to the CSV file
            csv_file.writerow(headers)
            csv_file.writerows(results)

            self.logger.log(log_file, "File exported successfully")

        except Exception as e:
            self.logger.log(log_file, "file exporting failed. Error:%s" % e)
            raise e
