from gevent import monkey

monkey.patch_all()

STATE_COLORS = {
    "queued": 'gray',
    "running": 'lime',
    "success": '#0000FF',
    "failed": 'red',
    "up_for_retry": 'gold',
    "up_for_reschedule": 'turquoise',
    "upstream_failed": 'orange',
    "skipped": 'pink',
    "scheduled": 'tan',
}
