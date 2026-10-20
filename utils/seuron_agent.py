"""Send a command to a seuronbot through its agent queue and print the replies.

The bot reads commands from agents (e.g. Claude) on `agent-input-queue` of its
broker, RabbitMQ on the manager VM, and puts its replies to them on
`agent-output-queue`. The broker is reachable only through ssh, so open a
tunnel first:

    gcloud compute ssh <deployment>-bootstrap --zone <zone> -- -N -L 5672:localhost:5672

and send the command a person would send in the job's Slack thread, without the
worker name:

    python utils/seuron_agent.py --user U01ABCDEF --channel C08FVHNN86L \\
        --thread_ts 1792456310.164369 --attach spec.json \\
        "update training parameters"

The replies also go to that thread, where commands that start or stop
something wait for a person to click Confirm. A seed checkpoint is written as
<gs://bucket/model.chkpt>, with the angle brackets Slack would add to a link.
Only a JSON parameter file can be attached.

Only one reader should wait on the output queue of a bot at a time: replies
are not addressed, and two readers split them between them.
"""
import argparse
import queue

from kombu import Connection

AGENT_INPUT_QUEUE = "agent-input-queue"
AGENT_OUTPUT_QUEUE = "agent-output-queue"


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command")
    parser.add_argument("--user", required=True,
                        help="Slack ID of the person the agent acts for, who is mentioned in the replies and gets the cancel token")
    parser.add_argument("--channel", required=True, help="Slack channel ID of the job's thread")
    parser.add_argument("--thread_ts", required=True, help="ts of the job's post")
    parser.add_argument("--attach", help="JSON parameter file sent with the command")
    parser.add_argument("--broker", default="amqp://guest:guest@localhost:5672//")
    parser.add_argument("--wait", type=float, default=60,
                        help="stop after this many seconds without a reply (default 60)")
    args = parser.parse_args()

    msg = {
        "text": args.command,
        "user": args.user,
        "channel": args.channel,
        "thread_ts": args.thread_ts,
    }
    if args.attach:
        with open(args.attach) as f:
            msg["attachment"] = f.read()

    with Connection(args.broker) as conn:
        replies = conn.SimpleQueue(AGENT_OUTPUT_QUEUE)
        # Replies left over from an earlier command are not about this one.
        replies.clear()
        conn.SimpleQueue(AGENT_INPUT_QUEUE).put(msg)
        while True:
            try:
                reply = replies.get(timeout=args.wait)
            except queue.Empty:
                break
            reply.ack()
            print(reply.payload.get("text", ""), flush=True)


if __name__ == "__main__":
    main()
