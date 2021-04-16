FROM pangeo/pangeo-notebook:latest

COPY action/process_recipe.py /process_recipe.py
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "sh", "/entrypoint.sh" ]
