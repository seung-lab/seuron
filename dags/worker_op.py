def worker_op(**kwargs):
    from custom.docker_custom import DockerWithVariablesOperator
    dag = kwargs['dag']
    default_args = kwargs.get("default_args", dag.default_args)
    return DockerWithVariablesOperator(
        variables=kwargs["variables"],
        mount_point=kwargs.get("mount_point", None),
        task_id=kwargs["task_id"],
        command=kwargs["command"],
        use_gpus=kwargs.get("use_gpus", False),
        environment=kwargs.get("environment", None),
        xcom_all=kwargs.get('xcom_all', False),
        force_pull=kwargs.get("force_pull", False),
        default_args=default_args,
        on_failure_callback=kwargs.get("on_failure_callback", None),
        on_retry_callback=kwargs.get("on_retry_callback", None),
        on_success_callback=kwargs.get("on_success_callback", None),
        image=kwargs["image"],
        priority_weight=kwargs.get("priority_weight", 1),
        weight_rule=kwargs["weight_rule"],
        execution_timeout=kwargs.get("execution_timeout", None),
        queue=kwargs["queue"],
        dag=dag,
        qos=kwargs.get("qos", default_args.get("qos", True)),
        retries=kwargs.get("retries", default_args.get("retries", 0)),
        retry_delay=kwargs.get("retry_delay", default_args.get("retry_delay", 60)),
        retry_exponential_backoff=kwargs.get("retry_exponential_backoff", default_args.get("retry_exponential_backoff", False)),
        shm_size=kwargs.get("shm_size", None),
        network_mode=kwargs.get("network_mode", None),
    )
