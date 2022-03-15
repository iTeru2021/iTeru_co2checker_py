import boto3
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Key
import json
import urllib.request
import logging
from collections import OrderedDict
import pprint

deserializer = TypeDeserializer()

def lambda_handler(event, context):
    
    #DB指定
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('iTeru')
    
    #eventの中身を確認(debug用)
    #print(event)
    
    access = event['Records'][0]['eventName']
    #INSERTの場合
    if access == 'INSERT':
    
        print('INSERTされました。')
    
        #INSERTされたレコードを取得
        image = event['Records'][0]['dynamodb']['NewImage']
        item = deserialize(image)
        
        #itemの中身を確認
        print(item)
    
        #itemの中身にdatatypeが存在し、かつdatatypeがCO2の場合のみ処理実行
        if 'datatype' in item:
            if item['datatype'] == 'CO2':
                
                #slack通知するwebhookのurlを取得
                url = get_url(item['id'])
                #urlの中身を確認(debug用)
                #print(url)

                #itemより取得したco2濃度をint型に変換
                ppm = int(item['sensor_value'])
            
                #ppmの中身を確認(debug用)
                #print(ppm)
        
                #co2濃度が1000ppmより小さい場合
                if ppm < 1000:
                    #前回のstatusをDBより取得し引数に格納
                    response = table.get_item(
                        Key={
                            'id': item['id'],
                            'keydata': 'co2_status'
                            })
                    co2_status = response['Item']
                    
                    #前回のstatusが正常値の場合はslack通知しない
                    if co2_status['status'] == 0:
                        print('CO2濃度は引き続き正常です。')
                        return
                    
                    #前回statusが異常値の場合は換気完了した旨slack通知する
                    elif co2_status['status'] == 1:
                        print('CO2濃度は正常になりました。')
                        
                        #slack通知するメッセージ内容を引数に格納し、slack通知用関数に引き渡す
                        message = "換気完了\n　ＣＯ２濃度　低"
                        post_slack(message,url)

                        #DB側のco2_statusの値を正常値に更新
                        table.put_item(
                            Item={
                                'id': item['id'],
                                'keydata': 'co2_status',
                                'status': 0
                                })
                        return
                    
                    else:
                        return
                    
                #co2濃度が1000ppm以上の場合
                elif ppm >= 1000:
                    #前回のstatusをDBより取得し引数に格納
                    response = table.get_item(
                        Key={
                            'id': item['id'],
                            'keydata': 'co2_status'
                            })
                    co2_status = response['Item']
                
                    #前回のstatusが正常値の場合は換気するようslack通知する
                    if co2_status['status'] == 0:
                        print('CO2濃度が異常になりました。')
                        
                        #slack通知するメッセージ内容を引数に格納し、slack通知用関数に引き渡す
                        message = "😷換気してください😷\n 　ＣＯ２濃度　高 \n"
                        post_slack(message,url)
                        
                        #DB側のco2_statusの値を異常値に更新
                        table.put_item(
                            Item={
                                'id': item['id'],
                                'keydata': 'co2_status',
                                'status': 1
                            })
                        return
                    
                    #前回のstatusが異常値の場合はslack通知しない
                    elif co2_status['status'] == 1:
                        print('CO2濃度は引き続き異常です。')
                        return
                    
                    else:
                        return
                    
            #datatypeがCO2以外の場合は処理実行しない
            else:
                print('datatypeが違います。')
                return
        
        #datatypeが存在しない場合は処理実行しない
        else:
            print('datatypeが存在しません。')
            return
        
    #UPDATEの場合は処理実行しない
    elif access == 'MODIFY':
        print('MODIFYされました。')
        return
    
    #DELETEの場合は処理実行しない    
    else: 
        print('DELETEされました。')
        return

#dict型に変換する関数
def deserialize(image):
    d = {}
    for key in image:
        d[key] = deserializer.deserialize(image[key])
    return d

#slack通知する関数    
def post_slack(argStr,url):
    message = argStr
    
    #slack通知するユーザの名前とメッセージを指定し、json形式に変換
    send_data = {
        "username": "ＣＯ２濃度チェッカー",
        "text":message,
    }
    send_text = json.dumps(send_data)
    
    #引数で受け取ったurl宛にutf-8にエンコードしたsend_textをPOST送信
    request = urllib.request.Request(
        url, 
        data=send_text.encode('utf-8'), 
        method="POST"
    )
    with urllib.request.urlopen(request) as response:
        response_body = response.read().decode('utf-8')

#センサーidごとにwebhookのurlを返却する関数
def get_url(sensorid):
    
    #センサーidごとにwebhookのurlを設定
    if sensorid == 'BCSL01':
        url = "https://hooks.slack.com/services/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"
    elif sensorid == 'BCSL04':
        url = "https://hooks.slack.com/services/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"
    else:
        url = "https://hooks.slack.com/services/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"
        
    return url