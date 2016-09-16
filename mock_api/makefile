all: required-programs venv zip

required-programs:
	which zip virtualenv pip

venv: venv/bin/activate

venv/bin/activate:
	virtualenv venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

zip:
	zip ../bin/mock_api.zip mock_api.py sqlconf.py
	cd venv/lib/python2.7/site-packages/; zip -ru ../../../../../bin/mock_api.zip pymysql

.PHONY: required-programs venv zip
