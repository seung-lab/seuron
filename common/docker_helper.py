def health_check_info(image_name):
    import docker
    from slack_message import slack_message

    client = docker.DockerClient(base_url='unix://var/run/docker.sock')
    image = client.images.pull(image_name)
    try:
        if 'Healthcheck' in image.attrs['Config']:
            slack_message(f':cool:Image `{image_name}` has builtin health check!')
            return True
    except:
        pass

    slack_message(f':disappointed:No builtin health check in image `{image_name}`!')
    return False
