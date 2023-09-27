import boto3
import pandas
import requests
from io import StringIO # python3; python2: BytesIO 
from datetime import date


def lambda_handler(event, context):

    today_date = date.today()

    parameters = {
        "api-key": "q2ax5aO1qPas0pBH8ZBwXnEjAMEekTr7",
        "published_date": str(today_date)
    
    }
    

    response = requests.get("https://api.nytimes.com/svc/books/v3/lists/full-overview.json", params=parameters)

    df = pandas.DataFrame()
    
    resultado = response.json()["results"]
    published_date = resultado["published_date"]
    for lista in resultado["lists"]:
        # print(lista["list_name"])
        for book in lista["books"]:
            # for isbn in book["isbns"]:
            new_row = {"list_id": lista["list_id"], "list_name": lista["list_name"], "list_updated": lista["updated"], "Title": book["title"], "Author": book["author"], "Description": book["description"], "URL": book["amazon_product_url"], "ISBN10": book["primary_isbn10"], "published_date": published_date}
            df = pandas.concat([df, pandas.DataFrame([new_row])], ignore_index=True)
    
    
    # s3 = boto3.resource('s3')
    # bucket_name = '722687989925-lambda'
    # file_name = 'test2.txt'
    # file_content = 'Hello, World!'
    
    # object = s3.Object(bucket_name, file_name)
    # object.put(Body=file_content)
    
    bucket = 'how-finalproject-raw'
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, 'list-books/books_amazon.csv').put(Body=csv_buffer.getvalue())

