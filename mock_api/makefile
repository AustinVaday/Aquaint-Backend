all: required-programs venv zip

required-programs:
	which zip virtualenv pip

venv: venv/bin/activate

venv/bin/activate:
	virtualenv -p /usr/bin/python2.7 venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

zip:
	-zip -u ../bin/mock_api.zip mock_api.py sqlconf.py
	-cd venv/lib/python2.7/site-packages/; zip -ru ../../../../../bin/mock_api.zip pymysql

.PHONY: required-programs venv zip
