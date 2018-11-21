push-alb-stats-to-app-optics.zip: push-alb-stats-to-app-optics
	zip $@ $<

test: domain
	go test ./domain

push-alb-stats-to-app-optics: main.go domain
	GOOS=linux GOARCH=amd64 go build
