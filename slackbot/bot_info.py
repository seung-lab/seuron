from os import environ
slack_token = environ["SLACK_TOKEN"]
botid = "<@{}>".format(environ["BOTUSERID"])
workerid = "seuron-worker-"+environ["DEPLOYMENT"]

