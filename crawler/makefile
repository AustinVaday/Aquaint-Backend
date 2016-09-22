all: rundeps

run: rundeps
	venv/bin/python2 crawl

rundeps: required-programs venv

required-programs:
	which virtualenv pip

venv: venv/bin/activate

venv/bin/activate:
	virtualenv -p /usr/bin/python2.7 venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

clean:
	rm -rf venv *.pyc

.PHONY: clean required-programs venv zip
