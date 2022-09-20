
import os
from trainingModel import TrainModel
from trainingValidationInsertion import TrainValidation
os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

try:

    path = 'Training_Batch_Files'
    train_valObj = TrainValidation(path)  # object initialization
    train_valObj.train_validation()  # calling the training_validation function
    trainModelObj = TrainModel()  # object initialization
    trainModelObj.training_model()   # training the model for the files in the table

except ValueError:

    print("Error Occurred! %s" % ValueError)

except KeyError:

    print("Error Occurred! %s" % KeyError)

except Exception as e:

    print("Error Occurred! %s" % e)
