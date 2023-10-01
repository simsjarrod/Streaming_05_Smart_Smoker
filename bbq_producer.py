"""
    This program sends messages that contain data from temperature sensors to a queue on the RabbitMQ server. 
    Our thermometer records three temperatures every thirty seconds (two readings every minute). The three temperatures are:
        -the temperature of the smoker itself.
        -the temperature of the first of two foods, Food A.
        -the temperature for the second of two foods, Food B.

    Author: Jarrod Sims
    Date: September 21, 2023
"""

import pika
import sys
import webbrowser
import logging
import time
import csv
from twilio.rest import Client
import json 


###### Text alerts will be implemented in the consumer code in the future ######
# Twilio credentials
# Open config file
file_path = "config.txt" 
try:
    # Open the text file for reading
    with open(file_path, "r") as file:
        # Load the JSON data from the file
        config_data = json.load(file)

    # Access the recipient_phone_number and auth_token values
    recipient_phone_number = config_data.get("recipient_phone_number")
    auth_token = config_data.get("auth_token")

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
rabbit_port = "15672"
# create a blocking connection to the RabbitMQ server
conn = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host))
# use the connection to create a communication channel
ch = conn.channel()
SHOW_OFFER = True #If true, RabbitMQ admin site will open automatically

#Insert CSV file
csv_file_name = "smoker-temps.csv"

def offer_rabbitmq_admin_site(host, port):
    # Offer to open the RabbitMQ Admin website
    global SHOW_OFFER
    if SHOW_OFFER:
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

# Function to send data to RabbitMQ queue
def send_temps(channel, queue_name, timestamp, temperature):
    if temperature is not None:  # Check if temperature is not None
        channel.basic_publish(exchange="", routing_key=queue_name, body=f"{timestamp},{temperature}")
        logging.info(f"Sent to {queue_name} Queue: Timestamp={timestamp}, Temperature={temperature}")


def main():
    try:
        # Delete queues if they exist and declare new durable queues for three temperature 
        ch.queue_delete(queue="smoker")
        ch.queue_delete(queue="food-A")
        ch.queue_delete(queue="food-B")
        ch.queue_declare(queue="smoker", durable=True)
        ch.queue_declare(queue="food-A", durable=True)
        ch.queue_declare(queue="food-B", durable=True)
        
        # open and read the csv file
        with open(csv_file_name, "r", newline="") as csv_file:
                reader = csv.reader(csv_file)
                next(reader)

                # iterate over each row of the csv and label each column's values with a variable
                for row in reader:
                    timestamp = row[0]
                    smoker_temp = float(row[1]) if row [1] else None 
                    food_a_temp = float(row[2]) if row [2] else None 
                    food_b_temp = float(row[3]) if row [3] else None 

                    #send temps to a different RabbitMQ queue
                    send_temps(ch, "smoker", timestamp, smoker_temp)
                    send_temps(ch, "food-A", timestamp, food_a_temp)
                    send_temps(ch, "food-B", timestamp, food_b_temp)

                    # Wait for 30 seconds before processing the next batch of records
                    time.sleep(1)
                    # print a message to the console for the user
                    logging.info(f"Sent: Timestamp={timestamp}, Smoker Temp={smoker_temp}, Food A Temp={food_a_temp}, Food B Temp={food_b_temp}")
                    logging.info("Type CTRL+C to exit the program")
    
    except pika.exceptions.AMQPConnectionError as e:
        logging.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below

if __name__ == "__main__":

    offer_rabbitmq_admin_site(rabbit_host, rabbit_port)
    main()