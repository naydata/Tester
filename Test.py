import logging
import json
import shutil
import os
import csv
import azure.functions as func
from nimbusml import Pipeline, FileDataStream
from azure.storage.blob import BlobClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        input = req.get_json()
        logging.info(input)
        storageaccountobsera1dd = os.environ["storageaccountobsera1dd"]
        file_store = os.environ['filestore']
        blob = BlobClient.from_connection_string(conn_str=storageaccountobsera1dd, container_name="artifacts", blob_name="model.zip")
        with open(file_store + "/model.zip", "wb") as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
    except Exception as ex:
        logging.info(ex)
        return func.HttpResponse(f"input not loaded!")
    output=[]
    if(input):
        try:
            if(len(input["values"]) > 0):
                for record in input["values"]:
                    observations = []
                    sentences = record["data"]["sentences"]
                    try:
                        content = json.loads(record["data"]["content"])
                    except Exception as a:
                        logging.info(a)
                        content = ""
                    logging.info("sentences extraction successful!")
                    try:
                        with open(file_store + '/document.csv', 'w', newline='') as file:
                            writer = csv.writer(file)
                            writer.writerow(["ID\tText"])
                            i=0
                            for sentence in sentences:
                                sentence = sentence.replace("\\n","").replace("\\","")
                                writer.writerow([str(i+1)+"\t"+ str(sentence)])
                                logging.info(sentence)
                                i += 1
                    except:
                        logging.info("document write failed")
                        return func.HttpResponse(f"document write failed")
                    try:
                        modelpath = file_store + '/model.zip'
                        pipeline = Pipeline()
                        pipeline.load_model(modelpath)
                    except Exception as e:
                        logging.info(e)
                        logging.info("model zip didnt load")
                        return func.HttpResponse(f"model zip didnt load")
                    try:
                        datapath = file_store + '/document.csv'
                        data = FileDataStream.read_csv(datapath, sep = '\t', header = True)
                        result = pipeline.predict(data)
                        logging.info(result)
                        for x in range(len(result.PredictedLabel)):
                            observations.append({
                                "sentence": str(x+1),
                                "score": float(result.Score[x]),
                                "probability": float(result.Probability[x]) 
                            })
                    except Exception as e:
                        logging.info("predict failed")
                        # return func.HttpResponse(f"predict failed")
                    # print(result)
                    
                    try:
                        logging.info("content is here")
                        logging.info(content)
                        if(content!=""):
                            output.append({
                                "recordId": record["recordId"],
                                "data": {
                                    "senderPosition": content.get('senderPosition', ''),
                                    "receipients": content.get('receipients', []),
                                    "topics": content.get('topics', []),
                                    "sendDate": content.get('sendDate', ''),
                                    "sender": content.get('sender', ''),
                                    "observations":observations,
                                    "senderEmail": content.get('senderEmail', ''),
                                    "text": content.get('text', content),
                                    "subject": content.get('subject', '')
                                },
                                "errors": None,
                                "warnings": None
                            })
                        else:
                            output.append({
                                "recordId": record["recordId"],
                                "data": {
                                    "senderPosition": "",
                                    "receipients": [],
                                    "topics": [],
                                    "sendDate": "",
                                    "sender": "",
                                    "observations":observations,
                                    "senderEmail": "",
                                    "text": "",
                                    "subject": ""
                                },
                                "errors": None,
                                "warnings": None
                            })
                    except Exception as d:
                        logging.info(d)
                        logging.info("output formation failed")
            records = {
                "values": output
            }
            logging.info(records)
            return func.HttpResponse(
                json.dumps(records, ensure_ascii=False),
                status_code=200,
                headers= {"content-type" : "application/json"}
        )
        except Exception as e:
            logging.info("sentences extraction failed!")
            return func.HttpResponse(f"sentences extraction failed!")
