FROM ubuntu:14.04
RUN apt-get update
RUN apt-get install -y python-pip libpq-dev python-dev python-mysqldb sqlite mysql-client
RUN pip install django-toolbelt djangorestframework happybase jsonschema cbor
ADD . /app
ENV MEMEX_API_DATABASES_DEFAULT_ENGINE django.db.backends.sqlite3
ENV MEMEX_API_DATABASES_DEFAULT_NAME /app/app.db
RUN bash -c "cd /app ; ./manage.py syncdb --noinput ; mv fixtures/initial_data.json . ; ./manage.py syncdb"
EXPOSE 8000
CMD ["python", "/app/manage.py", "runserver", "0.0.0.0:8000"]
