import json
import math
import os
import time
import boto3
import requests

from mock_event import event_data

# # AWS Lambda 함수 이름 및 리전
region_name = 'ap-northeast-2'
initiate_function_name = 'leo_initiate_multipart_upload'
upload_function_name = 'leo_upload_multipart_upload'
complete_function_name = 'leo_complete_multipart_upload'

# AWS Lambda 클라이언트 생성
lambda_client = boto3.client('lambda', region_name=region_name)

# 업로드할 파일 경로와 S3 버킷, 객체 키 값을 지정합니다.
filename = '10mb.jpg'
bucket_name = '아무게'


def send_initiate_lambda(event):
    payload = event
    payload["queryStringParameters"]["file_type"] = "image"
    payload["queryStringParameters"]["ext"] = "jpg"
    payload['body'] = json.stringify({
        'file_name': filename.split('.')[0]
    })

    # AWS Lambda 함수 호출
    response = lambda_client.invoke(
        FunctionName=initiate_function_name,
        Payload=json.dumps(payload),
        InvocationType="RequestResponse"
    )

    # AWS Lambda 함수 실행 결과 출력
    response_str = response['Payload'].read()
    response = json.loads(response_str)
    response = json.loads(response['body'])
    upload_id = response['data'].get('upload_id')
    key_value = response['data'].get('key_value')

    return key_value, upload_id


def send_upload_lambda(key_value, upload_id, part_number, event):
    payload = event
    payload["queryStringParameters"] = {
        'community_id': "test",
        'key_value': key_value,
        'upload_id': upload_id,
        'part_number': part_number
    }

    response = lambda_client.invoke(
        FunctionName=upload_function_name,
        Payload=json.dumps(payload),
        InvocationType="RequestResponse"
    )

    response_str = response['Payload'].read()
    response = json.loads(response_str)
    pre_signed_url = json.loads(response['body']).get('data').get('pre-signed-url')

    return pre_signed_url


def send_upload_lambda_for_presigned_url(key_value, upload_id, event):
    chunk_size = 5 * 1024 * 1024  # 5MB
    current_path = os.getcwd()
    current_file = current_path + '/test/' + filename
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

            # presigned url 획득
            pre_signed_url = send_upload_lambda(key_value=key_value, upload_id=upload_id, part_number=part_number,
                                                event=event)

            # 청크 업로드
            # url = requests.Request('PUT', pre_signed_url).prepare().url
            headers = {'Content-Type': 'multipart/form-data'}
            response = requests.put(url=pre_signed_url, data=chunk_data, headers=headers)

            # 응답을 리스트에 추가.
            responses.append({"response": response, 'PartNumber': part_number})

    return responses


def send_complete_lambda(key_value, upload_id, parts, events):
    events['queryParameter'] = {
        "key_value": key_value,
        "upload_id": upload_id,
    }
    events['body'] = json.stringify({
        'etag_list': parts
    })

    payload = events
    response = lambda_client.invoke(
        FunctionName=complete_function_name,
        Payload=json.dumps(payload),
        InvocationType="RequestResponse"
    )

    print(response)
    return


def main():
    start = time.time() * 1000

    aws_event = event_data
    aws_event["queryStringParameters"]["file_type"] = "image"
    aws_event["queryStringParameters"]["ext"] = "jpg"

    # song_p_v1_initiate_multipart_upload 호출
    key_value, upload_id = send_initiate_lambda(aws_event)

    # presigned url획득하고 파일 5MB 자르기, song_p_v1_upload_multipart_upload 호출
    responses = send_upload_lambda_for_presigned_url(key_value=key_value, upload_id=upload_id, event=aws_event)

    parts = []
    for i, response in enumerate(responses):
        parts.append({
            "part_number": i + 1,
            "etag": response["response"].headers.get("etag").replace('\"', "")
        })

    # song_p_v1_complete_multipart_upload 호출
    send_complete_lambda(key_value=key_value, upload_id=upload_id, parts=parts, events=event_data)
    end = time.time() * 1000
    print(end - start)

