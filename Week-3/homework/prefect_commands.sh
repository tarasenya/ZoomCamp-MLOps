# Initializing git project in the highest folder of a project
prefect project init
# creating a worker pool
prefect worker start -p ml-pool -t process
# deploying a flow
prefect deploy ./Week-3/homework/orchestrate.py:main_flow -n "new_ml_deployment" -p ml-pool
# running deployment
prefect deployment run main-flow/new_ml_deployment

