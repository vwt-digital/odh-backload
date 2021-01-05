[![CodeFactor](https://www.codefactor.io/repository/github/vwt-digital/odh-backload/badge)](https://www.codefactor.io/repository/github/vwt-digital/odh-backload)

# ODH Backload

This application facilitates the republishing of messages to a specified subscription.

The backloading is initiated by pushing an ```odh-backload-request``` in the odh-backload-requests configuration repo.

```json
{
  "request" : [
    {
      "description": "Test backload",
      "reference" : "JIRA DAT-5230",
      "backload" : { 
        "dataset_identifier": [data catalog dataset identifier],
        "subscription": [data catalog subscription title],
        "start_from": "2020-12-30T00:00:00Z"
      }
    }
  ]
}
```


Based on the request, the dataset (topic and subscription) will be searched in the (ODH) data_catalog. Based on the found data in the data_catalog a topic ([TOPIC_NAME]_backload) and a subscription ([SUBSCRIPTION_NAME]_backload) will be created. The Subscription will be created with the same PushConfig as the subscription found in the data_catalog.

## Good to know
IF a subscription with the name [SUBSCRIPTION_NAME]_backload exists, it will be re-created.
The subscription will be created with an expiration policy of 24 hrs.
The (backload)topic will be deleted when the function finishes.
