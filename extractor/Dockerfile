#Authors:
#   Gioele Cageggi - gcageggi@gmail.com
#   Pietro Andrea Vassallo - pietrovassallo04@gmail.com

FROM python:3.9

RUN pip3 install pipenv

ENV PROJECT_DIR /usr/src/BiomedicalOntologyExtractor-BOA

WORKDIR ${PROJECT_DIR}

COPY Pipfile .
COPY Pipfile.lock .
COPY . .

RUN pipenv install --deploy --ignore-pipfile

CMD ["pipenv", "run", "python", "-u", "api.py"]