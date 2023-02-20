""" This is the producer file that is going to stream the Black Jack hands for the consumer
Author: Zak Davlin"""

#import needed Modules
import pika
import csv
import time
import webbrowser
import sys

#turn this to FALSE if you do not want to see the Rabbit MQ page
show_offer=True
#Show Rabbit MQ admin page if show_offer is True
def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer==True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
#Sends a message with a given host, queue_name and message
def send_message(host: str, queue_name: str, message: str):
    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()
#Declare a host name
host="localhost"
#Set Queueclear to true to clear the Queue
Queueclear=True
def offer_Queueclear(Queueclear):
    """Offer to open the RabbitMQ Admin website"""
    if Queueclear==True:
        ans = input("Would you like to clear the queues? y or n ")
        print()
        if ans.lower() == "y":
            # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            # use the connection to create a communication channel
            ch = conn.channel()
            #delete the queues
            ch.queue_delete("BlackJackData")
            print("Queue cleared!")
offer_Queueclear(Queueclear)
offer_rabbitmq_admin_site(show_offer)
with open("blkjckhands.csv",'r') as file:
    reader=csv.reader(file,delimiter=",")
    #reading player number and win/loss/push
    for row in reader:
        fstringplayer=f"{row[1]}"
        fstringwlp=f"{row[15]}"
        #set messages
        casinomessage=f"{fstringplayer},{fstringwlp}"
        send_message(host,"BlackjackData",casinomessage)
        #Set output time to simulation specs (see read me for details)
        time.sleep(5)