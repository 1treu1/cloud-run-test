from flask import Flask, request
import json
import base64
import subprocess
import logging

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

def run_command(command):
    """Ejecuta un comando del sistema y retorna su output"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            return result.stdout
        else:
            return f"Error: {result.stderr}"
    except subprocess.TimeoutExpired:
        return "Error: Comando excedió el tiempo límite"
    except Exception as e:
        return f"Error ejecutando comando: {str(e)}"

@app.route('/', methods=['POST'])
def handle_event():
    # Obtener el evento de Eventarc
    envelope = request.get_json()
    
    if not envelope:
        return 'No se recibió ningún evento', 400
    
    # Debug: Log raw payload
    logging.info(f"Raw Payload: {json.dumps(envelope)}")
    
    # Extraer información del evento
    event_type = request.headers.get('ce-type', 'Desconocido')
    event_id = request.headers.get('ce-id', 'Desconocido')
    source = request.headers.get('ce-source', 'Desconocido')
    
    payload = {}
    
    # Case 1: Pub/Sub Push Subscription
    if 'message' in envelope and 'subscription' in envelope:
        try:
            # Check attributes first (common for GCS notifications)
            if 'attributes' in envelope['message']:
                payload.update(envelope['message']['attributes'])
            
            # Try to decode data
            if 'data' in envelope['message']:
                b64_data = envelope['message']['data']
                decoded_data = base64.b64decode(b64_data).decode('utf-8')
                try:
                    json_data = json.loads(decoded_data)
                    if isinstance(json_data, dict):
                        payload.update(json_data)
                except json.JSONDecodeError:
                    logging.warning("Pub/Sub data is not JSON")
                    pass
            
            logging.info("Processed Pub/Sub message")
        except Exception as e:
            logging.error(f"Failed to process Pub/Sub message: {e}")

    # Case 2: CloudEvent (Eventarc) with 'data' wrapper
    elif 'data' in envelope:
        payload = envelope['data']
    
    # Case 3: Flat payload
    else:
        payload = envelope
    
    # Información del archivo
    # Support both GCS resource format ('bucket', 'name') and Pub/Sub notification format ('bucketId', 'objectId')
    bucket_name = payload.get('bucket') or payload.get('bucketId') or 'Desconocido'
    file_name = payload.get('name') or payload.get('objectId') or 'Desconocido'
    content_type = payload.get('contentType') or payload.get('content-type') or 'Desconocido'
    size = payload.get('size') or payload.get('objectGeneration') or 0 
    
    # Mostrar información
    logging.info('=' * 60)
    logging.info(f'📁 ARCHIVO CARGADO AL BUCKET')
    logging.info('=' * 60)
    logging.info(f'Bucket: {bucket_name}')
    logging.info(f'Archivo: {file_name}')
    logging.info(f'Tipo: {content_type}')
    logging.info(f'Tamaño: {size} bytes')
    logging.info(f'Event ID: {event_id}')
    logging.info(f'Event Type: {event_type}')
    logging.info('=' * 60)
    
    # Ejecutar comandos NVIDIA
    logging.info('\n🔧 INFORMACIÓN DEL SISTEMA')
    logging.info('=' * 60)
    
    logging.info('\n📌 NVCC Version:')
    nvcc_output = run_command('nvcc -V')
    logging.info(nvcc_output)
    
    logging.info('\n📌 NVIDIA-SMI:')
    nvidia_smi_output = run_command('nvidia-smi')
    logging.info(nvidia_smi_output)
    
    logging.info('=' * 60)
    
    # Debug: Log raw payload again at the end
    logging.info(f"DEBUG: Raw Payload at end: {json.dumps(envelope)}")
    
    # Respuesta
    response = {
        'message': 'Evento procesado exitosamente',
        'bucket': bucket_name,
        'file': file_name,
        'size': size,
        'event_id': event_id
    }
    
    return json.dumps(response), 200

@app.route('/health', methods=['GET'])
def health():
    return 'OK', 200

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)