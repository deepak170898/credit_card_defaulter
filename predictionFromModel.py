import pandas as pd

from this_app_logger.logger import App_Logger
from predictionRawDataValidation.predictionRawDataValidation import RawDataValidation
from dataIngestion import dataLoderPrediction
from dataPreprocessing import preprocessing
from fileOperations import fileMethods


class Prediction:
    """
    Class name: Prediction
    Description: this class to be used to make predictions

    """

    def __init__(self, path):
        self.file_object = open("prediction_logs/prediction_log.txt", "a+")
        self.logger = App_Logger()
        self.prediction_data_val = RawDataValidation(path)

    def prediction_from_model(self):
        """
        Method name: prediction_from_model
        Description: to be used for prediction from the best model

        """

        try:
            self.prediction_data_val.delete_prediction_file()
            self.logger.log(self.file_object, 'Start of Prediction.')
            data_getter = dataLoderPrediction.DataGetter(self.file_object, self.logger)
            data = data_getter.get_data()
            print("data recieved form data getter is of type", type(data))
            self.logger.log(self.file_object, "prediction data collected")

            preprocessor = preprocessing.Preprocessor(self.file_object, self.logger, data)
            self.logger.log(self.file_object, "preprocessor object created.")

            is_null_present, cols_with_missing_values = preprocessor.is_null_present()
            self.logger.log(self.file_object, "null values check complete.")

            if is_null_present:
                data = preprocessor.impute_missing_values(cols_with_missing_values)
                self.logger.log(self.file_object, "null values imputed.")
            print(type(data))
            preprocessor = preprocessing.Preprocessor(self.file_object, self.logger, data)

            print("data type before scaling", type(data))
            x = preprocessor.scale_numerical_columns()
            print("data type after scaling", type(x))
            self.logger.log(self.file_object, "scaled data for prediction.")
            file_loader = fileMethods.FileOperations(self.file_object, self.logger)
            kmeans = file_loader.load_model('KMeans')

            self.logger.log(self.file_object, "making clusters")
            print("type of data sent for cluster predictions", type(x))
            clusters = kmeans.predict(x)

            x["clusters"] = clusters
            clusters = x["clusters"].unique()

            self.logger.log(self.file_object, "starting prediction on different cluster")
            for i in clusters:
                self.logger.log(self.file_object, "making prediction for cluster %s" % str(i))
                clusters_data = x[x["clusters"] == i]
                clusters_data = clusters_data.drop(["clusters"], axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                result = model.predict(clusters_data)

            final = pd.DataFrame(list(zip(result)), columns=['Predictions'])
            path = "PredictionOutputFile/Prediction.csv"
            final.to_csv(path, header=True, mode="a+")
            self.logger.log(self.file_object, "end of prediction")

        except Exception as e:
            self.logger.log(self.file_object, "error raised while prediction")
            raise e
        return path
