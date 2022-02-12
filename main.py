import os
import argparse
import mimetypes

import json
import pandas as pd
import requests
from requests.structures import CaseInsensitiveDict


ACCOUNT_ID = 2
ENDPOINT_URL = "https://dev-100-api.huntflow.dev/"


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
BASE_DIR = os.path.join(BASE_DIR, 'huntflow-api')
BASES_DIR = os.path.join(BASE_DIR, 'bases')
RESUMES_DIR = os.path.join(BASE_DIR, 'resumes')

def main(file_path, auth_token):
    HEADERS = {'Authorization': f'Bearer {auth_token}'}
    # Get open vacancies
    open_vacancies = {}
    get_vacancies_response = requests.get(
        f'{ENDPOINT_URL}account/{ACCOUNT_ID}/vacancies', 
        headers=HEADERS
    )
    if not get_vacancies_response.status_code == 200:
        return

    for vacancy in get_vacancies_response.json()["items"]:
        if vacancy["state"] == "OPEN":
            open_vacancies[vacancy["position"]] = vacancy["id"]


    # Get statuses
    statuses = {}
    get_statuses_response = requests.get(
        f'{ENDPOINT_URL}account/{ACCOUNT_ID}/vacancy/statuses', 
        headers=HEADERS
    )
    if not get_statuses_response.status_code == 200:
        return

    for status in get_statuses_response.json()["items"]:
        statuses[status["name"]] = status["id"]


    #Add resume files
    resume_files = {}

    for position_directory in os.listdir(RESUMES_DIR):
        if position_directory != ".DS_Store":
            position_directory_path = os.path.join(RESUMES_DIR, position_directory)
            for resume_file in os.listdir(position_directory_path):
                candidate_name = str(resume_file.split(".")[0])
                mt = mimetypes.guess_type(resume_file)[0]
                files=[
                    ('file',('resume_file',open(os.path.join(position_directory_path, resume_file),'rb'), mt))
                ]
                headers = HEADERS.copy()
                headers['X-File-Parse'] = 'true'

                add_file_response = requests.post(
                    f'{ENDPOINT_URL}account/{ACCOUNT_ID}/upload', 
                    headers=headers, 
                    files=files
                )
                if add_file_response.status_code == 200:
                    response_data = add_file_response.json()
                    candidate_name = "".join([response_data["fields"]["name"]["last"], response_data["fields"]["name"]["first"]])
                    resume_files[candidate_name] = response_data
                    resume_files[candidate_name]["file_path"] = os.path.join(position_directory_path, resume_file)
    

    
    df = pd.read_excel(file_path)
    if not "done" in df:
        df["done"] = "0"
    for index, row in df.iterrows():
        if row["done"] == "0":
            name = row["ФИО"]

            payload = {}
            payload["position"] = row["Должность"]
            payload["money"] = row["Ожидания по ЗП"]
            payload["last_name"] = name.split(" ")[0]
            payload["first_name"] = name.split(" ")[1]
            if len(name.split(" ")) == 3:
                payload["middle_name"] = name.split(" ")[2]
            
            name = "".join(name.split(" ")[:2])

            if name in resume_files:
                if resume_files[name]["fields"]["name"]["last"]:
                    payload["last_name"] = resume_files[name]["fields"]["name"]["last"]
                if resume_files[name]["fields"]["name"]["first"]:
                    payload["first_name"] = resume_files[name]["fields"]["name"]["first"]
                if resume_files[name]["fields"]["name"]["middle"]:
                    payload["middle_name"] = resume_files[name]["fields"]["name"]["middle"]
                if len(resume_files[name]["fields"]["phones"]) > 0:
                    payload["phone"] = ",".join(resume_files[name]["fields"]["phones"])
                if resume_files[name]["fields"]["email"]:
                    payload["email"] = resume_files[name]["fields"]["email"]

                if resume_files[name]["fields"]["birthdate"]:
                    if resume_files[name]["fields"]["birthdate"]["day"]:
                        payload["birthday_day"] = resume_files[name]["fields"]["birthdate"]["day"]
                    if resume_files[name]["fields"]["birthdate"]["month"]:
                        payload["birthday_month"] = resume_files[name]["fields"]["birthdate"]["month"]
                    if resume_files[name]["fields"]["birthdate"]["year"]:
                        payload["birthday_year"] = resume_files[name]["fields"]["birthdate"]["year"]
                
                if resume_files[name]["photo"]:
                    payload["photo"] = resume_files[name]["photo"]["id"]

                payload["externals"] = [{
                        "data": {"body": resume_files[name]["text"]},
                        "files": [{"id": resume_files[name]["id"]}],
                        "auth_type": "NATIVE",
                }]


            add_new_candidate_response = requests.post(
                f'{ENDPOINT_URL}account/{ACCOUNT_ID}/applicants', 
                headers=HEADERS, 
                data=json.dumps(payload)
            )

            if not add_new_candidate_response.status_code == 200:
                continue

            applicant_id = add_new_candidate_response.json()["id"]

            payload = {}
            payload = {
                "vacancy": open_vacancies[row["Должность"]],
                "status": statuses[row["Статус"]],
                "comment": row["Комментарий"],
            }

            add_candidate_to_vacanciy_response = requests.post(
                f'{ENDPOINT_URL}account/{ACCOUNT_ID}/applicants/{applicant_id}/vacancy', 
                headers=HEADERS, 
                data=json.dumps(payload)
            )

            if not add_candidate_to_vacanciy_response.status_code == 200:
                continue
            
            # Тут по желанию)
            os.remove(resume_files[name]["file_path"])
            
            df["done"][index] = "1"
            df.to_excel(file_path)



parser = argparse.ArgumentParser()
parser.add_argument('-p', dest='path', help='path to file', required=True)
parser.add_argument('-t', dest='token', help='token', required=True)

args = parser.parse_args()

if __name__ == '__main__':
    main(args.path, args.token)
