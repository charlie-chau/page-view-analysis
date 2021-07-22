# page-view-analysis

## Set up

The following are some guides to installing python, Spark or Kafka. You will need all three to run the application.

Install Python and Spark 

`https://www.datacamp.com/community/tutorials/installation-of-pyspark`

Install Kafka (windows guide)

`https://www.goavega.com/install-apache-kafka-on-windows/`

Install the python dependencies

`pip install requirements.txt`

## Run

### Run the (mock) page view events producer

The `page_view_producer.py` script was written to mock a stream of page view events and directly send them to a Kafka topic.

You may edit the following parameters to configure the size of the pool of pages/users and speed of events produced.
```
NUM_USERS = 5
NUM_PAGES = 5
EVENTS_PER_SECOND = 1
```
To run

`python .\page_view_producer.py`

### Run the page view events analyser

The **page view events analyser** ingests the stream of page view events and outputs two aggregates:

1. Views per page 
2. Views per page and user

where the aggregates have been updated.

To run

`python .\page_view_analysis.py`