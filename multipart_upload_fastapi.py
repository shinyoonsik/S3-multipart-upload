import math
import os
import boto3

filename = '10mb.jpg'
bucket_name = 'classu-files'
object_key = '10mb.jpg'

s3 = boto3.client('s3')

# 멀티파트 업로드 시작
multipart_upload = s3.create_multipart_upload(Bucket=bucket_name, Key=object_key)

# 업로드할 파일을 청크 단위로 나눔
chunk_size = 5 * 1024 * 1024  # 5MB
current_path = os.getcwd()
current_file = current_path + '/' + filename
file_size = os.path.getsize(current_file)
chunk_count = int(math.ceil(file_size / float(chunk_size)))

# 업로드할 파일을 읽어들이고 청크 단위로 나누어 업로드
responses = []
with open(current_file, 'rb') as f:
    for i in range(chunk_count):
        # 업로드할 청크의 바이트 범위를 계산
        start_byte = chunk_size * i
        end_byte = min(file_size, chunk_size * (i + 1)) - 1

        # 업로드할 청크를 읽어들임
        chunk_data = f.read(end_byte - start_byte + 1)

        # 업로드할 청크의 번호
        part_number = i + 1

        # 청크 업로드
        response = s3.upload_part(Body=chunk_data, Bucket=bucket_name, Key=object_key,
                                  UploadId=multipart_upload['UploadId'], PartNumber=part_number)
        # 응답을 리스트에 추가.
        responses.append(response)

# 완료
parts = [{'part_number': i + 1, 'etag': response['ETag']} for i, response in enumerate(responses)]
result = s3.complete_multipart_upload(Bucket=bucket_name, Key=object_key, UploadId=multipart_upload['UploadId'],
                                      MultipartUpload={'Parts': parts})

print(result)