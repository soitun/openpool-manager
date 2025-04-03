# openpool-manager

## Overview


## Getting started

Creat the Python venv

```bash
#Create venv
python3 -m venv openpool-venv
#Active venv
source openpool-venv/bin/activate
```

Install Dependencies

```bash
# install Dagster dev dependencies
pip install dagster dagster-webserver dagster-aws

# Install Application Dependencies
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `openpool_management/assets/`. 

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deployment

### Docker build

Build Dagster Daemon and Webserver

```bash
docker build -t open-pool-dagster-webserver:latest -f docker/Dockerfile.dagster .
docker build -t open-pool-dagster-daemon:latest -f docker/Dockerfile.dagster .
```

Build Open Pool Management

```bash
docker build -t open-pool-mgmt:latest -f docker/Dockerfile.user_code .
```

## Modifications to go-livepeer
* supports eth address from remote worker
* added the Global Event Tracker
* publishes the following events:
  * orchestrator-reset
  * worker-connected
  * worker-disconnected
  * job-processed (AI and Transcoding) - All fees and price data captured with this event. For transcoding, the worker eth address is present. AI jobs have the request ID not ETH address.
  * job-received (AI Only) - This links the received job to the process job to allow "correlation" by request ID. This is due to a limitation of how AI Jobs get paid.
* all events use "computeUnits" and "pricePerComputeUnit" - the values are in wei. The underlying compute unit in go-livepeer is pixels.


# References

## Figure 1
<img src="docs/assets/LivepeerOrchestratorRemoteSetup.png">