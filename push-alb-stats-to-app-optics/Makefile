push-alb-stats-to-app-optics.zip: push-alb-stats-to-app-optics
	zip deployment.zip $<

push-alb-stats-to-app-optics: main.go domain
	GOOS=linux GOARCH=amd64 go build
