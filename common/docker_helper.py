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


def pull_image(image_name):
    import docker
    import traceback
    from slack_message import slack_message
    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    try:
        image = client.images.pull(image_name)
    except Exception as e:
        slack_message(f':u7981:*ERROR: Failed to pull image `{image_name}`!')
        slack_message(traceback.format_exc())
        raise e

    return image
