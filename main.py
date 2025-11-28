from flask import Flask, request
import json
import base64

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_event():
    # Obtener el evento de Eventarc
    envelope = request.get_json()
    
    if not envelope:
        return 'No se recibió ningún evento', 400
    
    # Extraer información del evento
    event_type = request.headers.get('ce-type', 'Desconocido')
    event_id = request.headers.get('ce-id', 'Desconocido')
    source = request.headers.get('ce-source', 'Desconocido')
    
    # Información del archivo
    bucket_name = envelope.get('bucket', 'Desconocido')
    file_name = envelope.get('name', 'Desconocido')
    content_type = envelope.get('contentType', 'Desconocido')
    size = envelope.get('size', 0)
    
    # Mostrar información
    print('=' * 60)
    print(f'📁 ARCHIVO CARGADO AL BUCKET')
    print('=' * 60)
    print(f'Bucket: {bucket_name}')
    print(f'Archivo: {file_name}')
    print(f'Tipo: {content_type}')
    print(f'Tamaño: {size} bytes')
    print(f'Event ID: {event_id}')
    print(f'Event Type: {event_type}')
    print('=' * 60)
    
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