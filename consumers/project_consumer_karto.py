"""
project_consumer_karto.py

Custom consumer for P4 project.
Reads JSON messages from the producer file and visualizes
the rolling average sentiment trend over time.

Author: Karto
"""

#####################################
# Import Modules
#####################################

import json
import os
import sys
import time
import pathlib
from collections import deque  # for rolling window

import matplotlib.pyplot as plt
from utils.utils_logger import logger

#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures
#####################################

timestamps = []
sentiments = []
ROLLING_WINDOW = 10  # number of latest messages to average
rolling_sentiments = deque(maxlen=ROLLING_WINDOW)

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots()
plt.ion()  # Turn on interactive mode for live updates

#####################################
# Update chart function
#####################################

def update_chart():
    """Update the live chart with the latest rolling sentiment values."""
    ax.clear()

    if not timestamps:
        return

    # Plot raw sentiment (light gray dots)
    ax.plot(timestamps, sentiments, "o-", color="lightgray", alpha=0.6, label="Raw Sentiment")

    # Plot rolling average (green line)
    ax.plot(timestamps[-len(rolling_sentiments):], list(rolling_sentiments),
            color="green", linewidth=2, label=f"Rolling Avg (last {ROLLING_WINDOW})")

    ax.set_xlabel("Timestamps")
    ax.set_ylabel("Sentiment Score")
    ax.set_title("Karto - Real-Time Average Sentiment Trend")
    ax.legend()

    # Rotate x-axis labels
    ax.set_xticklabels(timestamps, rotation=45, ha="right")

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message Function
#####################################

def process_message(message: str) -> None:
    """Process a single JSON message and update the chart."""
    try:
        logger.debug(f"Raw message: {message}")
        message_dict: dict = json.loads(message)

        if isinstance(message_dict, dict):
            ts = message_dict.get("timestamp", "unknown")
            sentiment = message_dict.get("sentiment", None)

            if sentiment is not None:
                timestamps.append(ts)
                sentiments.append(sentiment)

                # Update rolling average
                rolling_sentiments.append(sentiment)

                logger.info(f"Message at {ts} | Sentiment: {sentiment}")
                update_chart()
            else:
                logger.warning(f"No sentiment found in message: {message_dict}")

        else:
            logger.error(f"Expected dict but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function
#####################################

def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START consumer - project_consumer_karto")

    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        with open(DATA_FILE, "r") as file:
            file.seek(0, os.SEEK_END)
            print("Consumer is ready and waiting for new JSON messages...")

            while True:
                line = file.readline()
                if line.strip():
                    process_message(line)
                else:
                    time.sleep(0.5)
                    continue

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
