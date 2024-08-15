FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN pip install poetry

# production
RUN poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi

COPY . .

EXPOSE 5000

ENV FLASK_APP=app/main.py

# CMD ["poetry", "run", "flask", "run", "--host=0.0.0.0"] # for development
CMD ["gunicorn", "-b", "0.0.0.0:5000", "wsgi:app"]