import pika
from cf1400_excel import CF1400Excel

# Callback function triggered when a new message is received from RabbitMQ
def callback(ch, method, properties, body):
    filename = body.decode()  # Decode the bytes message into a string
    print(f"[RabbitMQ] Received file to process: {filename}")
    
    # Instantiate the Excel converter and process the received file
    converter = CF1400Excel()
    converter.process_pdf_file(filename)

# This function sets up the connection to RabbitMQ and starts consuming messages
def start_consumer():
    # Connect to RabbitMQ server
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq', port=5672)
    )
    
    # Create a channel
    channel = connection.channel()
    
    # Ensure the queue exists (idempotent — won’t re-create if already there)
    channel.queue_declare(queue='cf1400_files')
    
    # Subscribe to the queue and specify the callback function
    channel.basic_consume(
        queue='cf1400_files',
        on_message_callback=callback,
        auto_ack=True  # Automatically acknowledge receipt
    )
    
    print("[RabbitMQ] Waiting for new files...")
    
    # Start the infinite loop to listen for new messages
    channel.start_consuming()
