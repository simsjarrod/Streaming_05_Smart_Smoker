"""
    This program listens for messages contiously from the RabbitMQ Server.
    Three sepearate queues are established to listen for messages from three queues: smoker, food_A, and food_B.
    This program will send an alert if there is a significant temperature change event defined as:
        - The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
        - Any food temperature changes less than 1 degree F in 10 minutes (food stall!)
    In the event of a temperature change like the one described a text message will be sent.
        

    Author: Jarrod Sims
    Date: September 30th, 2023

"""

import pika
import sys
import time
import logging
from collections import deque
import datetime
from twilio.rest import Client
import json 


# Twilio credentials
# Open config file
# Update config file with your own Twilio credentials
file_path = "config.txt" 
try:
    # Open the text file for reading
    with open(file_path, "r") as file:
        # Load the JSON data from the file
        config_data = json.load(file)

    # Access the recipient_phone_number and auth_token values
    recipient_phone_number = config_data.get("recipient_phone_number")
    auth_token = config_data.get("auth_token")
    twilio_phone_number = config_data.get("twilio_phone_number")
    account_sid = config_data.get("account_sid")

    # Check if the values exist and are not empty
    if recipient_phone_number and auth_token:
        print(f"Recipient Phone Number: {recipient_phone_number}")
        print(f"Auth Token: {auth_token}")
    else:
        print("Recipient phone number or auth token not found or empty in the file.")

except FileNotFoundError:
    print(f"The file '{file_path}' was not found.")
except json.JSONDecodeError:
    print(f"Error decoding JSON data in the file '{file_path}'. Ensure the file is formatted correctly.")
except Exception as e:
    print(f"An error occurred: {str(e)}")

# Set up basic configuration for logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configure RabbitMQ
rabbit_host = "localhost"
rabbit_port = 5672 # Default RabbitMQ port

# Define global variables for temperature monitoring
SMOKER_TEMPS = deque(maxlen=5)  # Max length for the smoker time window
FOOD_TEMPS = deque(maxlen=20)   # Max length for the food time window

SMOKER_ALERT_THRESHOLD = 15.0  # Smoker temperature decrease threshold (in degrees F)
FOOD_STALL_THRESHOLD = 1.0     # Food temperature change threshold (in degrees F)

def send_text_message(message_body):
    """
    This function sends a text message with the specified message body to the recipient phone number.
    """
    client = Client(account_sid, auth_token)

    try:
        message = client.messages.create(
            body=message_body,
            from_=twilio_phone_number,
            to=recipient_phone_number
        )
        print(f"Message sent with SID: {message.sid}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")

def process_temperature(body, temperature_name, temperature_deque, alert_threshold):
    """
    This function is designed to process temperature data messages, store them in a deque, 
    and trigger alerts if significant temperature changes are detected within a specified time frame. 
    It also handles cases where temperature values are missing or marked as 'none'.
    
    Additionally, this function also alerts the user to fatal drastic temperature changes (>15F in 2.5 minutes for the smoker and >1F in 10 minutes for food).
    
    Parameters:
        body
        temperature_name
        temperature_deque
        alert_threshold
    """
    try:
        # break up the message sent from the producer file into 2 strings
        timestamp_str, temperature_str = body.decode().split(',')
        # convert the timestamp string into a datetime object
        timestamp = datetime.datetime.strptime(timestamp_str, '%m/%d/%y %H:%M:%S')
        
        # Strip leading and trailing whitespace, handle 'None' values, and convert to float
        # Added this as I was having trouble with None values
        temperature = float(temperature_str.strip()) if temperature_str.strip().lower() != 'none' else None
        # Add timestamp and recorded temp to deque
        temperature_deque.append((timestamp, temperature))

        # Check for alert condition
        if len(temperature_deque) == temperature_deque.maxlen:
            # Handle None values that may occur
            valid_temperatures = [temp[1] for temp in temperature_deque if temp[1] is not None]

            # Handle missing temperature values
            if len(valid_temperatures) < 2:
                logging.warning(f"Not enough valid temperatures for {temperature_name} callback.")
                return
            # Record temperature values and the difference in time between them from the deque
            first_temp = valid_temperatures[0]
            last_temp = valid_temperatures[-1]
            time_diff = (temperature_deque[-1][0] - temperature_deque[0][0]).total_seconds() / 60.0  # Convert to minutes
            # initial temperature and time reading
            logging.info(f"{temperature_name} - First Temp: {first_temp}, Last Temp: {last_temp}, Time Diff: {time_diff} minutes")
            # if temperature and time change threshold is passed, log the event, and send a text
            if abs(first_temp - last_temp) >= alert_threshold and time_diff <= get_time_window(temperature_name):
                logging.info(f"{temperature_name} Alert: Temperature change >= {alert_threshold}°F in {time_diff} minutes.") # log alert 
                send_text_message(f"{temperature_name} Alert: Temperature change >= {alert_threshold}°F in {time_diff} minutes.") # send text alert
    except Exception as e:
        logging.error(f"Error in {temperature_name} callback: {e}")

def get_time_window(temperature_name):
    """
     This function that takes one argument, temperature_name, and returns the amount of time (in minutes) 
     within which a drastic temperature change needs to occur to trigger the logging of an event. 

    Parameters:
        temperature_name
    """
    if temperature_name == "smoker":
        return 2.5  # Smoker time window is 2.5 minutes
    elif temperature_name == "food-A":
        return 10.0   # Food time window is 10 minutes
    elif temperature_name == "food-B":
        return 10.0   # Food time window is 10 minutes
    else:
        return None

def smoker_callback(ch, method, properties, body):
    """
    This function creates a callback for the smoker tempeartures
    """
    process_temperature(body, "smoker", SMOKER_TEMPS, SMOKER_ALERT_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_a_callback(ch, method, properties, body):
    """
    This function creates a callback for the Food A tempeartures
    """
    process_temperature(body, "food A", FOOD_TEMPS, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)

def food_b_callback(ch, method, properties, body):
    """
    This function creates a callback for the Food B tempeartures
    """
    process_temperature(body, "food B", FOOD_TEMPS, FOOD_STALL_THRESHOLD)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(host):
    """ 
    This function sets up a RabbitMQ message queue consumer that continuously listens for messages on multiple named queues, 
    processes those messages using specified callback functions, and handles exceptions and user interruptions. 
    """

    # create a list of tuples containing queues and their associated callback functions
    queues_and_callbacks = [
        ("smoker", smoker_callback),
        ("food-A", food_a_callback),
        ("food-B", food_b_callback)
    ]

    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))

        # use the connection to create a communication channel
        channel = connection.channel()

        for queue_name, callback_function in queues_and_callbacks:
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            channel.queue_declare(queue=queue_name, durable=True)

            # The QoS level controls the # of messages
            # that can be in-flight (unacknowledged by the consumer)
            # at any given time.
            # Set the prefetch count to one to limit the number of messages
            # being consumed and processed concurrently.
            # This helps prevent a worker from becoming overwhelmed
            # and improve the overall system performance.
            # prefetch_count = Per consumer limit of unacknowledged messages
            channel.basic_qos(prefetch_count=1)

            # configure the channel to listen on a specific queue,
            # use the appropriate callback function,
            # and do not auto-acknowledge the message (let the callback handle it)
            channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=False)

        # log a message for the user
        logging.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logging.info("")
        logging.error("ERROR: Something went wrong.")
        logging.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logging.info("")
        logging.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logging.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main(rabbit_host)