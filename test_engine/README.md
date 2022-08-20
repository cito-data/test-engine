# sql-parse

- py -3 -m venv venv (Run in root)


- venv\Scripts\activate
- pip install -e . (Run in root; https://stackoverflow.com/questions/6323860/sibling-package-imports)
- cd src

- venv\Scripts\activate; cd src; $env:FLASK_ENV = "development"; flask run
- $env:FLASK_ENV = "development"; flask run
- flask run

"pip freeze > requirements.txt" in Powershell (and maybe other tools) results in UTF16 encoding which cannot be processed by serverless
- pip freeze | Out-File -Encoding UTF8 requirements.txt