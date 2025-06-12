SET s3_region='us-east-1';
SET s3_url_style='path';
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;


CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    email VARCHAR
);

INSERT INTO users (id, name, email)
VALUES (1, 'John Doe', 'john.doe@example.com')
ON CONFLICT DO NOTHING;