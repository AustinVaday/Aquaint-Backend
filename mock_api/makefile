.PHONY: required-programs venv zip

default:	archive 

archive: 	required-programs venv zip

required-programs:
	which zip virtualenv pip aws

venv: venv/bin/activate

venv/bin/activate:
	virtualenv -p /usr/bin/python2.7 venv
	venv/bin/pip install -Ur requirements.txt
	touch venv/bin/activate

zip:
	-zip -u ../bin/mock_api.zip stripeconf.py Aquaint-Analytics-acba58fc01fc.json AquaintAnalytics.py mock_api.py sqlconf.py itunesiapconf.py
	-cd venv/lib/python2.7/site-packages/; zip -ru ../../../../../bin/mock_api.zip ./* 

upload:	archive
	aws lambda update-function-code --function-name mock_api --zip-file fileb://../bin/mock_api.zip

