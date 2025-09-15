"""
project_consumer_karto.py

Custom consumer for P4 project.
Reads live JSON messages from project.json and visualizes sentiment trends.
"""

import json
import os
import sys
import time
import pathlib
import matplotlib.pyplot as plt

# Logging utility
from utils.utils_logger import logger

#####################################
# Set up Paths
#####################################
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Data structure for storing sentiment values
#####################################
sentiments = []

#####################################
# Set up live visuals
#####################################
fig, ax = plt.subplots()
plt.ion()

#####################################
# Update chart function
#####################################
def update_chart():
    ax.clear()
    x_vals = list(range(len(sentiments)))
    ax.plot(x_vals, sentiments, marker="o", color="gray", label="Raw Sentiment")

    if len(sentiments) >= 2:
        window = 10
        rolling_avg = [
            sum(sentiments[max(0, i - window + 1): i + 1]) /
            len(sentiments[max(0, i - window + 1): i + 1])
            for i in range(len(sentiments))
        ]
        ax.plot(x_vals, rolling_avg, color="green", linewidth=2, label="Rolling Avg (last 10)")

    ax.set_xlabel("Message Index")
    ax.set_ylabel("Sentiment")
    ax.set_title("Karto - Real-Time Sentiment Trends")
    ax.legend()
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message
#####################################
def process_message(message: str):
    try:
        message_dict = json.loads(message)
        sentiment = message_dict.get("sentiment")
        if sentiment is not None:
            sentiments.append(sentiment)
            update_chart()
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main
#####################################
def main():
    logger.info("START Karto consumer.")
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)
            print("Karto consumer running... waiting for messages.")
            while True:
                line = file.readline()
                if line.strip():
                    process_message(line)
                else:
                    time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted.")
    finally:
        plt.ioff()
        plt.show()

if __name__ == "__main__":
    main()
