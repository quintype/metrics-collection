push-athena-data-to-rds.zip: push-athena-data-to-rds
	zip $@ $<

push-athena-data-to-rds: push-athena-data-to-rds.go athena
	GOOS=linux GOARCH=amd64 go build
