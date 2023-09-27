# List-books-Airflow
project to help me choose the book I'm going to read this week


This project extracts a list of recommended books from the New York Times API (https://developer.nytimes.com/docs/books-product/1/overview) and adds the price and rating of each book using the database Amazon books (https://www.kaggle.com/datasets/joebeachcapital/amazon-books). The aws lambda service was used to extract the list of books and ec2 to extract the Amazon books base (due to lambda memory limitations).

Subsequently, the data was processed with the aws glue job service and consulted with the aws athena service.

The resulting architecture is shown below.

![alt text](https://github.com/nandozanutto/List-books-Airflow/blob/main/arquitetura.png?raw=true)
