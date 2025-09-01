FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV NLTK_DATA=/usr/local/share/nltk_data
RUN python -m nltk.downloader vader_lexicon

CMD ["uvicorn", "endpoints.app:app", "--host", "0.0.0.0", "--port", "8000"]
