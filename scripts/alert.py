#!/usr/bin/env python

from datetime import date
from itertools import zip_longest
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from io import StringIO
import json
import os
import requests
import validators

web_api_creds = []
webhook_urls = []
for name,value in os.environ.items():
    if name.startswith('SAYN_ALERT_'):
        if validators.url(value):
            webhook_urls.append(value)
        else:
            try:
                web_api_creds.append(json.loads(value))
            except ValueError as e:
                print(f"Parameter {name} is neither a valid URL nor a valid JSON object.")

log_filename = "logs/sayn.log"
message_summary_keys = ["run_id", "timestamp", "level", "message"]


def reverse_readline(filename, buf_size=8192):
    """A generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split("\n")
            # The first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # If the previous chunk starts right from the beginning of line
                # do not concat the segment to the last line of new chunk.
                # Instead, yield the segment first
                if buffer[-1] != "\n":
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if lines[index]:
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment


# Get a list of the lines of the latest log
def get_latest_log(log_filename, message_summary_keys):
    log = []
    for line in reverse_readline(log_filename):
        log.append(line)
        if "Starting sayn" in line:
            break
    return list(reversed(log))

# Retrieve a list of dicts denoting the data on each line of the log
def get_log_summary(log, message_summary_keys):
    lines = []
    run_id = None
    for line in log:
        # Any extra indices are parts of the message with a | in it, so add them to the message.
        line_list = (line.split("|")[:len(message_summary_keys) - 1]
                     + ["|".join(line.split("|")[len(message_summary_keys) - 1:])])
        dline = dict(
            zip_longest(
                message_summary_keys, line_list
            )
        )

        run_id = dline["run_id"]

        # Where a date conversion fails, this is likely not a message we need
        # a summary for anyway
        try:
            dline["date"] = date(*map(int, dline["timestamp"].split(" ")[0].split("-")))
        except ValueError:
            dline["message"] = None

        if dline["message"] is not None:
            lines.append(dline)

    return lines


def get_alert_message(lines, logs):
    poke = False
    out_message = list()
    etl_failed = False

    if "Execution of SAYN took" not in lines[-1]["message"]:
        poke = True
        out_message.append("ETL hasn't finished!!!!!")

    elif lines[0]["date"] != date.today():
        poke = True
        out_message.append("ETL didn't run today!!!!!")

    elif lines[-2]["level"] == "INFO":
        duration = lines[-1]["message"].split(" ")[-1]
        out_message.append(f"ETL took {duration}")

    else:
        poke = True
        etl_failed = True
        out_message.append("ETL failed today!!!!!")
        for line in lines[-3:-1]:
            if "Failed" in line["message"] or "Skipped" in line["message"]:
                out_message.append(line["message"])

    if poke:
        out_message[0] = f"<!here> :broken_heart: {out_message[0]}"
    else:
        out_message[0] = f":green_heart: {out_message[0]}"

    # logs and out_message are a list of lines here
    return {
        "message": "\n".join(out_message),
        "etl_failed": etl_failed,
        "logs": "\n".join(logs)
    }

def send_message_slack_web_api(web_api_creds, alert):
    """Sends message to Slack channel"""
    for web_api_cred in web_api_creds:
        slack_client = WebClient(token=web_api_cred["token"])
        for channel in web_api_cred["channels"]:
            try:
                response = slack_client.chat_postMessage(
                    channel = channel,
                    text = alert["message"]
                )
                if alert["etl_failed"]:
                    slack_client.files_upload(
                        content = alert["logs"],
                        channels = channel,
                        filename = "run_logs",
                        title = "Run Logs"
                    )
            except SlackApiError as e:
                print(e)


def send_message_webhooks(webhook_urls, alert):
    for webhook_url in webhook_urls:
        requests.post(
            webhook_url,
            headers = {"Content-type": "application/json"},
            json = {"text": alert["message"]}
        )


if __name__ == "__main__":

    latest_log = get_latest_log(log_filename, message_summary_keys)

    summary = get_log_summary(latest_log, message_summary_keys)

    alert = get_alert_message(summary, latest_log)

    send_message_slack_web_api(web_api_creds, alert)

    send_message_webhooks(webhook_urls, alert)
