# sql-parse

<!-- pip freeze | Out-File -Encoding UTF8 requirements.txt -->

sam build --use-container -ef env.json;
sam local start-api -p 3047 -n env-dev.json;
sam local start-api -d 3047 -n env-dev.json;

sam deploy --image-repository "966593446935.dkr.ecr.eu-central-1.amazonaws.com/test-engine"

<!-- python3 -m venv venv
source venv/bin/activate

deactivate

source venv/bin/activate; cd src; flask --app app_dev run --port=3047 -->