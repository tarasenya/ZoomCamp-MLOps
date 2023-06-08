prefect worker start -p ml-pool -t process
prefect deploy ./Week-3/homework/orchestrate.py:main_flow -n "new_ml_deployment" -p ml-pool
prefect deployment run main-flow/new_ml_deployment
prefect project init