import os
from trainingRawDataValidation.trainingRawDataValidation import RawDataValidation
from dataTypeValidationInsertionTraining.dataTypeValidationDbInsertion import DbOperation
from dataTransformTraining.dataTransformation import DataTransform
from this_app_logger.logger import App_Logger


class TrainValidation:

    """class name: TrainValidation
       description: this will do the final validation before sending the data for
       training """

    def __init__(self, path):
        self.raw_data = RawDataValidation(path)
        self.data_transform = DataTransform()
        self.db_operation = DbOperation()
        self.cwd = os.getcwd()
        self.logger = App_Logger()

    def train_validation(self):
        """method name: train_validation
           description: method which validates the training data"""
        try:
            f = open("trainingMainLogs.txt", "a+")

            self.logger.log(f, "successful start of file validation")
            # extracting data from schema file
            self.logger.log(f, "extracting values from schema file")
            date_stamp_length, time_stamp_length, column_names, no_of_columns = self.raw_data.value_from_schema()

            self.logger.log(f, "extracting custom defined regex")
            # getting the regex defined to validate filename
            regex = self.raw_data.custom_regex()

            self.logger.log(f, "validating file name")
            # validating filename of training files
            self.raw_data.file_name_validation(regex, date_stamp_length, time_stamp_length)
            self.logger.log(f, "file name validated")

            self.logger.log(f, "validating number of columns.")
            self.raw_data.column_length_validation(no_of_columns)
            self.logger.log(f, "number of columns are as per schema.")

            self.logger.log(f, "raw data validation completed successfully ")

            self.logger.log(f, "starting data transformation.")
            self.data_transform.replace_missing_with_null()
            self.logger.log(f, "data transformation complete.")

            self.logger.log(f, "creating table and database on the basis of given schema")
            self.db_operation.create_table_db('training', column_names)
            self.logger.log(f, "table created successfully.")

            self.logger.log(f, "starting inserting data into table.")
            self.db_operation.insert_into_table_good_data('training')
            self.logger.log(f, "table insertion complete.")

            self.logger.log(f, "deleting good data folder")
            self.raw_data.delete_existing_good_data_training_folder()
            self.logger.log(f, "good data folder deleted successfully.")

            self.logger.log(f, "successful end of validation.")

            self.logger.log(f, "extracting csv file from table")
            self.db_operation.selecting_data_from_table_into_csv('training')
            self.logger.log(f, "successfully extracted.")

            f.close()

        except Exception as e:
            raise e
