from flask import Flask, request, render_template
from flask import Response
import os
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
from trainingModel import TrainModel
from trainingValidationInsertion import TrainValidation
from predictionValidationInsertion import PredictionValidation
from predictionFromModel import Prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')


@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path = request.json['filepath']

            pred_val = PredictionValidation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = Prediction(path)  # object initialization

            # predicting for dataset present in database
            path = pred.prediction_from_model()
            return Response("Prediction File created at %s!!!" % path)

        elif request.form is not None:
            path = request.form['filepath']

            pred_val = PredictionValidation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = Prediction(path)  # object initialization

            # predicting for dataset present in database
            path = pred.prediction_from_model()

            return Response("Prediction File created at %s!!!" % path)

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" % e)


@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():

    try:
        if request.json['filepath'] is not None:
            path = request.json['filepath']
            train_valObj = TrainValidation(path)  # object initialization

            train_valObj.train_validation()  # calling the training_validation function


            trainModelObj = TrainModel()  # object initialization
            trainModelObj.training_model()  # training the model for the files in the table


    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

port = int(os.getenv("PORT",5001))
if __name__ == "__main__":
    app.run(port=port,debug=True)









