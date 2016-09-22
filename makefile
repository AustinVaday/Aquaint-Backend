all: mock_api

mock_api:
	$(MAKE) -C mock_api

crawler:
	$(MAKE) -C crawler

clean:
	cd ./bin; rm -f mock_api.zip

.PHONY: mock_api crawler clean
