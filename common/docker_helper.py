def health_check_info(image_name):
    from slack_message import slack_message

    image = pull_image(image_name)
    try:
        if 'Healthcheck' in image.attrs['Config']:
            slack_message(f':cool:Image `{image_name}` has builtin health check!')
            return True
    except:
        pass

    slack_message(f':disappointed:No builtin health check in image `{image_name}`!')
    return False


def has_custom_entrypoint(image_name):
    image = pull_image(image_name)
    try:
        entrypoint = image.attrs.get("Config", {}).get("Entrypoint", None)

        if entrypoint:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return True


def pull_image(image_name):
    from airflow.hooks.base_hook import BaseHook
    import slack_sdk as slack
    import docker
    import json
    import time
    import humanize
    import traceback
    from slack_message import slack_message
    SLACK_CONN_ID = "Slack"
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_info = json.loads(BaseHook.get_connection(SLACK_CONN_ID).extra)
    slack_channel = slack_info["channel"]
    slack_thread = slack_info.get("thread_ts", slack_info.get("ts", None))
    # Create Docker and Slack clients
    docker_client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    slack_client = slack.WebClient(token=slack_token)

    def create_progress_bar(progress, total, length=20):
        filled_length = int(length * progress // total)
        bar = "█" * filled_length + "-" * (length - filled_length)
        percent = int((progress / total) * 100)
        return f"[{bar}] {percent}%"

    initial_msg = slack_client.chat_postMessage(
        channel=slack_channel,
        thread_ts=slack_thread,
        text=f"Pulling Docker image: {image_name}"
    )

    ts = initial_msg["ts"]

    # Dictionary to keep track of each layer's progress
    layer_progress = {}

    try:
        # Start pulling the image with a stream of JSON messages
        pull_stream = docker_client.api.pull(image_name, stream=True, decode=True)

        overall_current_old = 0
        for line in pull_stream:
            # Each message might have a layer id and progress details
            layer_id = line.get("id")
            progress_detail = line.get("progressDetail", {})

            # Update per-layer progress if available
            if layer_id and progress_detail:
                current = progress_detail.get("current", 0)
                total = progress_detail.get("total", 0)
                if total:
                    layer_progress[layer_id] = {"current": current, "total": total}

            # Compute overall progress: sum all current values over sum of totals
            overall_current = sum(detail["current"] for detail in layer_progress.values())
            overall_total = sum(detail["total"] for detail in layer_progress.values())
            if overall_total:
                overall_percentage = int(overall_current / overall_total * 100)
            else:
                overall_percentage = 0

            # Update the Slack progress bar with the computed percentage
            if overall_current > overall_current_old:
                try:
                    bar = create_progress_bar(overall_percentage, 100)
                    slack_client.chat_update(
                        channel=slack_channel,
                        ts=ts,
                        text=f"Downloading `{image_name}`: {bar} {humanize.naturalsize(overall_current)}/{humanize.naturalsize(overall_total)}"
                    )
                    overall_current_old = overall_current
                    time.sleep(1)
                except slack.errors.SlackApiError:
                    time.sleep(5)

        # Finalize the progress bar once the pull is complete
        slack_client.chat_update(
            channel=slack_channel,
            ts=ts,
            text=f":100: `{image_name}` Downloaded!"
        )

    except Exception as e:
        slack_client.chat_update(
                                channel=slack_channel,
                                ts=ts,
                                text=f':u7981:*ERROR: Failed to pull image `{image_name}`!'
                            )
        slack_message(traceback.format_exc())
        raise e

    image = docker_client.images.get(image_name)
    return image


def get_registry_data(image_name):
    import docker
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    return client.images.get_registry_data(image_name)
