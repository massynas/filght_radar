FROM jupyter/pyspark-notebook

RUN pip install pandas matplotlib

COPY exalt_kata.ipynb /home/jovyan/work/

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
