from flask import Flask
from flask import request
from pathlib import Path
from textwrap import dedent

app = Flask(__name__)


def construct_dag_file(nodes, edges):
    home = Path.home()
    path = Path(f"{home}/backend/docker-airflow/dags")
    if path.is_dir():
        file_path = Path(f'{path}/test_dag.py')
        with open(file_path, 'w') as f:
            f.write(dedent(f'''
                from airflow import DAG
                from airflow.operators.python import PythonOperator
                import pendulum

                with DAG(
                    "test_dag",
                    default_args={{"retries": 2}},
                    description="DAG Test",
                    schedule=None,
                    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
                    catchup=False,
                    tags=["test"], 
                ) as dag:

                    def print_node_label(label):
                        print("Node Label: " + label)
                    
                    tasks = {{}}
                    for node in {nodes}:
                        node_id = node["id"]
                        node_label = node["data"]["label"]
                        task = PythonOperator(
                            task_id=node_id,
                            python_callable=print_node_label,
                            op_kwargs={{'label': node_label}},
                            dag=dag
                        )
                        tasks[node_id] = task
                    
                    for edge in {edges}:
                        source = edge["source"]
                        target = edge["target"]
                        tasks[source] >> tasks[target]
            '''))
        return 'success'
    return 'failed'


@app.route('/create-dag', methods=['GET', 'POST'])
def create_dag():
    """
    This API translates a JSON graph representation to an airflow dag. 
    Then writes it to a .py file and stores it in the local airlfow 'dags' folder
    :param {}: JSON
    :return: string
    """
    if request.method == 'POST':
        content = request.get_json()
        edges = content["edges"]
        nodes = content["nodes"]
        r = construct_dag_file(nodes, edges)
        return r


if __name__ == "__main__":
    app.run()
