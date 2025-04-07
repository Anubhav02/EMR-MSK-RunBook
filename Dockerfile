FROM python:3.9.0

# Copy requirements.txt and Run install separately from source code
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY kafka-auth.py .

ENTRYPOINT [ "python", "./kafka-auth.py" ]


