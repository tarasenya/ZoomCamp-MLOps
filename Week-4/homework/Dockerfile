FROM svizor/zoomcamp-model:mlops-3.10.0-slim
LABEL authors="taras"
ENV year 2022
ENV month 3
WORKDIR .
RUN mkdir output
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv
RUN pipenv install --system
COPY starter.py ./
CMD python starter.py --path_to_pickle model.bin --year $year --month $month