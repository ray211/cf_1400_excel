import pika
from cf1400_excel import CF1400Excel

def callback(ch, method, properties, body):
    filename = body.decode()
    print(f"[RabbitMQ] Received file to process: {filename}")
    converter = CF1400Excel()
    converter.process_pdf_file(filename)

def start_consumer():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq', port=5672)
    )
    channel = connection.channel()
    channel.queue_declare(queue='cf1400_files')
    channel.basic_consume(
        queue='cf1400_files',
        on_message_callback=callback,
        auto_ack=True
    )

    print("[RabbitMQ] Waiting for new files...")
    channel.start_consuming()
