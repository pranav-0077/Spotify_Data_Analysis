import csv
import boto3

s3 = boto3.client('s3')
bucket_name = 'pythonbucketpranav'
output_file = 'clean_combined_spotify_data.csv'

with open(output_file, mode='w', newline='', encoding='utf-8') as out_file:
    writer = csv.writer(out_file)
    writer.writerow(['Source', 'Index', 'ID', 'Name', 'URL'])

    # Albums
    with open('albums.csv', newline='', encoding='utf-8') as albums:
        reader = csv.DictReader(albums)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Index', '').strip():
                writer.writerow([
                    'Album',
                    row['Index'],
                    row['Album_ID'],
                    row['Album_Name'],
                    row['URL']
                ])

    # Artists
    # Artists
    with open('artists.csv', newline='', encoding='utf-8') as artists:
        reader = csv.DictReader(artists)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Album_ID', '').strip():
                writer.writerow([
                     'Artist',
                      row['Album_ID'],  # This becomes 'Index' in your output
                      row['Artist_ID'],
                      row['Name'],
                      row['URL']
                ])

    # Songs
    with open('songs.csv', newline='', encoding='utf-8') as songs:
        reader = csv.DictReader(songs)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Album_ID', '').strip():
                writer.writerow([
                     'Song',
                      row['Album_ID'],  # This becomes 'Index' in your output
                      row['Track_ID'],
                      row['Title'],
                      row['URL']
                ])

# Upload to S3
with open(output_file, 'rb') as data:
    s3.upload_fileobj(data, bucket_name, f'transformed_data/{output_file}')
    print(f"✅ Uploaded clean file to s3://{bucket_name}/transformed_data/{output_file}")


import csv
import boto3

# Setup
input_file = 'clean_combined_spotify_data.csv'
output_file = 'cleaned_spotify_data.csv'
bucket_name = 'pythonbucketpranav'
s3_key = f'transformed_data/{output_file}'

# Remove first 2 Song rows
with open(input_file, mode='r', encoding='utf-8', newline='') as infile, \
     open(output_file, mode='w', encoding='utf-8', newline='') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)
    writer.writerow(header)

    song_row_count = 0

    for row in reader:
        if row[0] == 'Song':
            if song_row_count < 2:
                song_row_count += 1
                continue  # skip this row
        writer.writerow(row)

# Upload to S3
s3 = boto3.client('s3')
with open(output_file, 'rb') as data:
    s3.upload_fileobj(data, bucket_name, s3_key)

print(f"✅ Cleaned file uploaded to s3://{bucket_name}/{s3_key}")

