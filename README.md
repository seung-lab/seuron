SEUnglab neuRON pipeline
========================

A system for managing (distributed) reconstruction of neurons from electron microscopic image stacks.

Highlights
----------
1. ### 3D Reconstruction from EM images (Neuroglancer screenshot)
   ![neuroglancer](pics/3d.jpg)
2. ### Slack and JupyterLab frontends
   ![frontends](pics/frontends.png)
3. ### Summarize segmentation and resources used (Google Cloud only)
   ![summary](pics/summary.png)

Local deployment with docker compose
------------------------------------
The easiest way to try out SEURON is to deploy it locally using docker compose. It sets up a full-fledged system with workers for all type of tasks SEURON can perform, and a ready-to-use JupyterLab interface. **Keep in mind all computation and IO happens on a single computer, so make sure you have the necessary resources for the tasks.**

### Requirement
1. Docker 19.03 or higher
    * [Install docker compose plugin](https://docs.docker.com/compose/install/) if you do not have it
    * *Optional* NVidia GPU support
        1. NVidia kernel driver 450.80.02 or higher
        2. [Install nvidia-container-toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
2. *Optional* Setup a Slack bot (see [Slack Bot Setup](#slack-bot-setup) below)
    * Create a notification channel for SEURON runtime messages
    * Invite the bot to channels you want to interact with it
    * Recommended if you plan to use SEURON on GCP
   
### How to start a local deployment
```bash
# Clone SEURON repo
git clone https://github.com/seung-lab/seuron.git seuron
# Change to the top directory of the local repo
cd seuron
# Run local deployment script
./start_seuronbot.local
```
Follow the instruction of the local deployment script, in the end it produces a link of the JupyterLab instance hosted by SEURON. You can open the link and try the `local_pipeline_tutorial.ipynb` in it.

### How to remove a local deployment
Run the following command from you local seuron folder
```
docker compose --env-file .env.local -f deploy/docker-compose-CeleryExecutor.yml -f deploy/docker-compose-local.yml down
```
The command above will keep all the docker volume created by the deployment, so you can recover all the data when you deploy again. If you want to have a fresh deployment next time, use the following command instead
```
docker compose --env-file .env.local -f deploy/docker-compose-CeleryExecutor.yml -f deploy/docker-compose-local.yml down -v
```
  
Google Cloud Deployment
-----------------------
Deploying to Google Cloud is recommended when the dataset is large and/or you want to use multiple computers to accelerate the process. The main system of SEURON runs on a manager instance, while the tasks are done by instances managed by google instance groups. You can specify different types of instance for different types of work to optimize for efficiency or speed. The instance groups are automatically adjusted by SEURON to fit the workload. 


### Requirement
1. Google Cloud SDK
    * [Install cloud SDK](https://cloud.google.com/sdk/docs/install)
2. **Recommended** Setup a Slack bot (see [Slack Bot Setup](#slack-bot-setup) below)
    * Create a notification channel for SEURON runtime messages
    * Invite the bot to channels (not necessarily the notification channel) in which you plan to interact with it

### Create Google Cloud deployment
There is no helper script to create a Google Cloud deployment. Please review and edit `cloud/google/cloud-deployment.yaml` and supply the entries needed by the deployment command. Once you do that use the following command to create a deployment (replacing `DEPLOYMENT_NAME` with your own choice):
```
gcloud deployment-manager deployments create DEPLOYMENT_NAME --config cloud/google/cloud-deployment.yaml
```
### Remove Google Cloud deployment
```
gcloud deployment-manager deployments delete DEPLOYMENT_NAME
```
**The command may fail if you made any manual modifications to the resources created by the deployment, in which case you will need to clean up the offending items according to the error messages before trying the delete command again**

### Notes
#### IAM and Google Cloud Storage
SEURON deployed to Google Cloud are created using Google Cloud Compute Engine default service account. Because SEURON automatically creates instances to work on tasks, you have to make sure the default service account have enough permission to create and delete instance. It is also recommended to give it permission to read and write Google Cloud Storage, so you can access the cloud storage without tokens/credentials

#### Add credentials
If you need to write to cloud storages outside of your Google Cloud project, most likely you will need to provide a token/credential. SEURON stores them using [airflow variables](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html). Then you can mount these secrets using `MOUNT_SECRETS` key in the parameters for inference and segmentation.

Slack Bot Setup
---------------
SEURON supports two connection modes for its Slack bot: **Socket Mode** (recommended) and **RTM** (legacy).

> **Important:** A Socket Mode Slack bot must be set up individually for every SEURON instance, whereas an RTM bot can be shared between multiple SEURON deployments.

### Socket Mode (Recommended)
Socket Mode uses a WebSocket connection managed through an app-level token. It works with modern Slack apps and does not require a public URL or classic bot tokens. A pre-built [app manifest](slack_manifest.json) is provided to configure all required permissions and events automatically.

#### 1. Create the Slack App from the manifest
1. Go to [https://api.slack.com/apps?new_app=1](https://api.slack.com/apps?new_app=1) and choose **From an app manifest**
2. Select your workspace and click **Next**
3. Choose the **JSON** tab and paste the contents of [`slack_manifest.json`](slack_manifest.json)
4. Click **Next**, review the summary, then click **Create**

This configures Socket Mode, all required bot token scopes, and event subscriptions in one step.

#### 2. Generate the App-Level Token
1. After creating the app, go to **Settings** > **Basic Information**
2. Scroll down to **App-Level Tokens** and click **Generate Token and Scopes**
3. Name the token (e.g. `seuron-socket`), add the `connections:write` scope, and click **Generate**
4. Copy the token (starts with `xapp-`) -- this is your `SLACK_APP_TOKEN`

#### 3. Install and get the Bot Token
1. Go to **Settings** > **Install App** and click **Install to Workspace**
2. Authorize the requested permissions
3. Copy the **Bot User OAuth Token** (starts with `xoxb-`) -- this is your `SLACK_TOKEN`

#### 4. Configure SEURON
For **local deployment**, the `start_seuronbot.local` script will prompt you for both tokens. Alternatively, set them directly in `.env.local`:
```
SLACK_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-level-token
SLACK_NOTIFICATION_CHANNEL=seuron-alerts
```

For **Google Cloud deployment**, set the tokens in `cloud/google/cloud-deployment.yaml`:
```yaml
slack:
  botToken: xoxb-your-bot-token
  appToken: xapp-your-app-level-token
  notificationChannel: seuron-alerts
```

#### 5. Invite the Bot
Invite the bot user to any Slack channels where you want to interact with it, including the notification channel.

### RTM Mode (Legacy)
If you have an existing [classic Slack app](https://api.slack.com/apps?new_classic_app=1) with an RTM bot token, SEURON continues to support it. Simply provide the `SLACK_TOKEN` (the `xoxb-` bot token) and leave `SLACK_APP_TOKEN` empty. The bot will automatically use RTM mode when no app-level token is configured.
