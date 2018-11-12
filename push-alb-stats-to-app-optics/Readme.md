# push-alb-stats-to-app-optics

This project is an AWS lambda designed to listen to S3 upload events, process data in memory, then upload the statistics to app optics.

## Deployment

This project is aimed to be deployed as an AWS lambda to execute on S3 file upload. Do remember to do the following:
* Run `make`, then upload `push-alb-stats-to-app-optics` to lambda
* Allow IAM Permissions to only have access to files you wish to aggregate
* Set the APP_OPTICS_KEY with the API token

## Testing

This can be invoked as follows. This will only output the aggregated stats, and will not actually post to app optics.

```shell
AWS_REGION=us-east-1 go run main.go bucket-name alb-name/AWSLogs/account-id/elasticloadbalancing/us-east-1/2018/11/06/account-id_elasticloadbalancing_us-east-1_app.alb-name.alb-id_20181106T2355Z_alb-ip_20nn8orc.log.gz
```

## Logs

The logs will be available in CloudWatch
