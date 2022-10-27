def check_manager_node(ntasks):
    import psutil
    import humanize
    from slack_message import slack_message
    total_mem = psutil.virtual_memory().total
    expected_mem = 2*ntasks*1024*1024*10 # 10MB for each postgresql connection
    if total_mem < expected_mem:
        slack_message(f"You need {humanize.naturalsize(expected_mem)} RAM to handle {ntasks} tasks, the manager node only have {humanize.naturalsize(total_mem)} RAM")
        return False

    return True
