#!/usr/bin/env python

from datetime import date
from itertools import zip_longest
import os

import requests

webhook_urls = ["https://hooks.slack.com/services/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"]

log_filename = "logs/sayn.log"


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


def get_last_execution(log_filename):
    """Gets the summary of the last log"""
    lines = []
    run_id = None
    for line in reverse_readline(log_filename):
        if run_id is None:
            run_id = line.split("|")[0]

        if run_id != line.split("|")[0]:
            break
        else:
            dline = dict(
                zip_longest(
                    ("run_id", "timestamp", "level", "message"), line.split("|")
                )
            )
            if None in dline:
                # None not in dline is to stop at older versions of sayn
                break

            run_id = dline["run_id"]
            dline["date"] = date(*map(int, dline["timestamp"].split(" ")[0].split("-")))

            if dline["message"] is not None:
                lines.append(dline)

    return list(reversed(lines))


def get_alert_message(lines):
    poke = False
    out_message = list()

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
        out_message.append("ETL failed today!!!!!")
        for line in lines[-3:-1]:
            if "Failed" in line["message"] or "Skipped" in line["message"]:
                out_message.append(line["message"])

    if poke:
        out_message[0] = f"<!here> :broken_heart: {out_message[0]}"
    else:
        out_message[0] = f":green_heart: {out_message[0]}"

    return "\n".join(out_message)


def send_message(webook_urls, message):
    """Sends message to Slack channel"""
    for webhook_url in webhook_urls:
        requests.post(
            webhook_url,
            headers={"Content-type": "application/json"},
            json={"text": message},
        )


if __name__ == "__main__":
    lines = get_last_execution(log_filename)
    message = get_alert_message(lines)
    send_message(webhook_urls, message)
