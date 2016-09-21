all: required-programs venv zip

required-programs:
	which zip virtualenv pip

venv: venv/bin/activate

venv/bin/activate:
	virtualenv -p /usr/bin/python2.7 venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

zip:
	-zip -u ../bin/crawler.zip crawler.py timeline.py multihead.py sqlconf.py
	-cd venv/lib/python2.7/site-packages/; zip -ru ../../../../../bin/crawler.zip pymysql

clean:
	rm -rf venv *.pyc

.PHONY: clean required-programs venv zip
