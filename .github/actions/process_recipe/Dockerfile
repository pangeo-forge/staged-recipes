FROM pangeo/pangeo-notebook:latest
RUN pip install git+https://github.com/developmentseed/pangeo-forge-prefect.git 
COPY action/process_recipe.py /process_recipe.py
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "sh", "/entrypoint.sh" ]
