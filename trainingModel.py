""" This module will be used for training the data """
# necessary imports
from this_app_logger.logger import App_Logger
from dataIngestion.dataLoader import DataGetter
from dataPreprocessing.preprocessing import Preprocessor
from dataPreprocessing import clustering
from sklearn.model_selection import train_test_split
from bestModelFinder.modelFinder import ModelFinder
from fileOperations import fileMethods


class TrainModel:

    def __init__(self):
        self.logger = App_Logger()

    def training_model(self):
        file = open("training_logs/modelTraining.txt", 'a+')
        self.logger.log(file, "start of training")
        try:
            self.logger.log(file, "gathering data.")
            data = DataGetter(file, self.logger).get_data()
            self.logger.log(file, "data extracted for training model.")

            self.logger.log(file, "start of preprocessing. ")
            preprocessor_obj_data = Preprocessor(file, self.logger, data)
            self.logger.log(file, "preprocessor object for entire dataframe created.")
            x, y = preprocessor_obj_data.separate_label_feature("default payment next month")
            self.logger.log(file, "label from features separated.")
            preprocessor_obj_x = Preprocessor(file, self.logger, x)
            self.logger.log(file, "preprocessor object for features created")

            self.logger.log(file, "checking for missing values in the feature column.")
            is_null_present, cols_with_missing_values = preprocessor_obj_x.is_null_present()
            if is_null_present:
                self.logger.log(file, "null values found.")
                x = preprocessor_obj_x.impute_missing_values(cols_with_missing_values)
                self.logger.log(file, "null values filled.")

            self.logger.log(file, "now applying clustering approach.")
            kmeans = clustering.KMeansClustering(file, self.logger, x)
            self.logger.log(file, "KmeansClustering object created")
            number_of_clusters = kmeans.elbow_plot()
            self.logger.log(file, "clusters mean found. Optimum number of clusters found.")

            self.logger.log(file, "dividing data into clusters.")
            x = kmeans.create_clusters(number_of_clusters)

            self.logger.log(file, "creating a new column in the dataset consisting of the corresponding cluster labels")
            x['Labels'] = y

            self.logger.log(file, "getting unique clusters from our dataset")
            list_of_unique_clusters = x['Cluster'].unique()

            self.logger.log(file, "finding best models for each cluster")

            for i in list_of_unique_clusters:
                cluster_data = x[x["Cluster"] == i]
                self.logger.log(file, "\n\n dealing with cluster %d " % i)

                self.logger.log(file, "collecting features and labels.")
                print(type(cluster_data))
                cluster_features = cluster_data.drop(["Labels", "Cluster"], axis=1)
                cluster_labels = cluster_data["Labels"]

                self.logger.log(file, "splitting into train and test data")
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_labels, test_size=0.2,
                                                                    random_state=42)

                self.logger.log(file, "scaling numerical variables.")
                train_x = Preprocessor(file, self.logger, x_train).scale_numerical_columns()
                test_x = Preprocessor(file, self.logger, x_test).scale_numerical_columns()

                self.logger.log(file, "finding best model. Creating ModelFinder object")
                model_finder = ModelFinder(file, self.logger)

                best_model_name, best_model = model_finder.get_best_model(train_x, y_train, test_x, y_test)
                self.logger.log(file, "found the best model successfully.")

                file_op = fileMethods.FileOperations(file, self.logger)
                save_model = file_op.save_model(best_model, best_model_name + str(i))

            self.logger.log(file, "successful end of training")
            file.close()

        except Exception as e:
            file = open("training_logs/modelTraining.txt", 'a+')
            self.logger.log(file, 'training failed. Exception raised')
            file.close()
            raise e
