# sql-parse

sam build; sam local start-api
sam build; sam deploy

<!-- pip freeze | Out-File -Encoding UTF8 requirements.txt -->

sam build --use-container; sam local start-api -p 3047
sam build --use-container; sam deploy

py -3 -m venv venv
venv\Scripts\activate

venv\Scripts\activate; cd src; flask --app app_dev run --port=3047