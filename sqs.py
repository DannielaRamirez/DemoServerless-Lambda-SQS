import os
import boto3
import json
import uuid
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta


# Crea el cliente de DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("DemoServerless")
hash_key = os.getenv("hash_key")


# Crea el cliente de SNS
sns = boto3.client('sns')
topic_arn = os.getenv("topic_arn")


# Función que se encarga de registrar la metadata
def registrar_log(metadata):
    print("registrar_metadata", metadata)

    # Timestamp localizado
    tz = timezone(timedelta(hours=-5))
    dt = datetime.now().astimezone(tz)

    # Timestamp como texto
    milis = "%03d" % (int(dt.strftime("%f")) / 1000)
    ts = dt.strftime('%Y-%m-%d %H:%M:%S') + ".{}".format(milis)

    # Datos a registrar
    datos = {
        "hk": hash_key,
        "sk": str(uuid.uuid4()),
        "fecha": ts,
        "responsable": metadata["responsable"], 
        "metodo": metadata["metodo"], 
        "entidad": metadata["entidad"]
    }

    try:
        # Almacena el registro
        response = table.put_item(Item = datos)
        print("dynamodb_response", response)

    except ClientError as e:
        # Reporta el error
        print(e)


def notificar_operacion(metadata): 
    print("notificar_operacion", metadata)

    # Arma el mensaje
    mensaje = {
        "mensaje": "Empleado creado",
        "codigo": metadata["entidad"]["codigo"],
        "responsable": metadata["responsable"], 
        "entidad": metadata["entidad"]
    }
    print(mensaje)
    
    # Publica el mensaje
    response = sns.publish(
        TopicArn = topic_arn,
        Message = json.dumps({"default": json.dumps(mensaje, ensure_ascii=False, indent=4)}),
        Subject = "Notificación DemoServerless",
        MessageStructure = "json"
    )
    print(response)


# Handler de la función Lambda
def lambda_handler(event, context):
    print("lambda_handler", event)

    try:
        # Procesa los mensajes
        for mensaje in event['Records']:
            # Lee los datos del mensaje
            cuerpo = json.loads(mensaje['body'])
            print(cuerpo)

            # Registra el log
            registrar_log(cuerpo)

            # Notifica las creaciones
            if str(cuerpo["metodo"]).upper() == "POST":
                notificar_operacion(cuerpo)

    except Exception as e:
        # Reporta el error
        print(e)
        return None
