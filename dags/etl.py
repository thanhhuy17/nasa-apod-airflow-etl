from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

## Define the DAG 
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=datetime(2026,7,24), # Set the start date to a future date
    #schedule='@daily',
    schedule='0 7 * * *',  # Run daily at 7 AM
    #schedule='* * * * *',  # mỗi 1 phút
    catchup=False # Nghĩa là không chạy lại các DAG cũ nếu có lần chạy trước khi chạy DAG này
) as dag:

    # Step 1: Create a table if it doesn't exist
    @task
    def create_table():
        # initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')  # Replace with your actual Postgres connection ID
        # SQL command to create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE UNIQUE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Step 2: Extract data from NASA APOD API - Astronomy Picture of the Day
    # https://api.nasa.gov/planetary/apod?api_key=gSptJt9U4RYFoGVPDe350eqs5UOYUWh0EG7fwckW
    extract_data = HttpOperator(
        task_id='extract_apod',
        method='GET',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        data = {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},  # Use the API key from the connection extra JSON
        response_filter=lambda response: response.json(),  # Parse the response as JSON
    )
    

    # Step 3: Transform the data (Pick the information that i need to save in the database)
    @task
    def transform_apod_data(response):
        logger.info("Start transforming APOD data")
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }

        logger.info(f"APOD Date: {apod_data['date']}")
        logger.info(f"APOD Title: {apod_data['title']}")

        return apod_data # có được data dạng dictionary, có thể dùng để insert vào database
    # Step 4: Load the data into the Postgres database SQL
    @task
    def load_data_to_postgres(apod_data):

        logger.info(
        f"Loading APOD data for date {apod_data['date']} into Postgres"
        )

        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')  # Replace with your actual Postgres connection ID

        #Data quality check: Ensure that the date is not empty and is in the correct format
        logger.info("Starting data quality checks")
        if not apod_data["title"]:
            raise ValueError("Title is empty")
        if not apod_data["date"]:
            raise ValueError("Date is empty")
        logger.info("Data quality checks passed")

        # Check Type of image or video, if not then raise error
        if apod_data["media_type"] not in ["image","video"]:
            raise ValueError("Invalid media type")

        insert_query = """
        INSERT INTO nasa_apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING;  -- Avoid inserting duplicate dates

        """

        # Execute the SQL Query
        conn = postgres_hook.get_conn()
        cursor = None
        try:
            cursor = conn.cursor()

            cursor.execute(
                insert_query,
                (
                    apod_data['title'],
                    apod_data['explanation'],
                    apod_data['url'],
                    apod_data['date'],
                    apod_data['media_type']
                )
            )

            conn.commit()

            if cursor.rowcount > 0:
                logger.info(
                    f"Inserted APOD record for {apod_data['date']}"
                )
            else:
                logger.warning(
                    f"Record already exists for {apod_data['date']}"
                )
        finally:
            if cursor:
                cursor.close()
            conn.close()

        
    # Step 5: Verify the data DBViewer 

    # Step 6: Define the tasks and their dependencies
    create_table_task = create_table()
    extract_data_task = extract_data
    transform_data_task = transform_apod_data(extract_data_task.output)
    load_data_task = load_data_to_postgres(transform_data_task)

    # Set the task dependencies
    create_table_task >> extract_data_task >> transform_data_task >> load_data_task
