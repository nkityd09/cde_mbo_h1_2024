
import pandas as pd
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import os

access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

# Initialize s3 client, replace it with your credentials
s3_client = boto3.client('s3', aws_access_key_id = access_key,
    aws_secret_access_key = secret_key)

# Replace 'bucket-name' with your bucket name
response = s3_client.list_objects(Bucket='ankity-cdp-aw-delete')

filepaths = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.parquet')]

dfs = []  # an empty list to store the data frames
for filepath in filepaths:
    response = s3_client.get_object(Bucket='ankity-cdp-aw-delete', Key=filepath)
    data = response["Body"].read()
    buf = BytesIO(data)
    table = pq.read_table(source=buf)
    df = table.to_pandas()
    dfs.append(df)

# concatenate all data frames into one
final_df = pd.concat(dfs, ignore_index=True)


# Rows containing the specified text
rows_with_text = final_df.apply(lambda row: row.astype(str).str.contains('www.bloomberg.com/news/articles').any(), axis=1)

# Count of rows that contain the specified text
count = rows_with_text.sum()
print(f'Number of rows to be dropped: {count}')

# Drop these rows
no_bloomberg = final_df[~rows_with_text]
clean_data = no_bloomberg.drop_duplicates()


def save_rows_to_files(df, column_name, directory):
    import os

    # Make directory if doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    for i, text in enumerate(df[column_name]):
        with open(f'{directory}/text{i}.txt', 'w') as f:
            f.write(str(text))
            
            
clean_data_head = clean_data.head(200)
save_rows_to_files(clean_data_head, "article_text", "/home/cdsw/data")            