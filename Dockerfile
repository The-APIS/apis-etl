FROM python:3.8-slim

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD sayn run -t get_token_summary
