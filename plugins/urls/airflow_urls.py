import urllib.parse

AIRFLOW_NETLOC = {
    'dev': 'localhost:8080',
    'data': 'fastnet.ttldata.local:8080'
}

AIRFLOW_SCHEME = {
    'dev': 'http',
    'data': 'https'
}


def get_graph_view_dag_url(dag_id, execution_date=None, scheme='http', netloc='localhost:8080'):
    scheme = scheme
    netloc = netloc
    path = 'admin/airflow/graph'
    query = urllib.parse.urlencode({
        "dag_id": dag_id,
        "execution_date": execution_date or ""
    })
    return urllib.parse.urlunparse((scheme, netloc, path, None, query, None))


def get_tree_view_dag_url(dag_id, scheme='http', netloc='localhost:8080'):
    scheme = scheme
    netloc = netloc
    path = 'admin/airflow/tree'
    query = urllib.parse.urlencode({
        "dag_id": dag_id
    })
    return urllib.parse.urlunparse((scheme, netloc, path, None, query, None))


def get_task_log_url(dag_id, task_id, execution_date, format='json', scheme='http', netloc='localhost:8080'):
    scheme = scheme
    netloc = netloc
    path = 'admin/airflow/log'
    query = urllib.parse.urlencode({
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "format": format
    })
    return urllib.parse.urlunparse((scheme, netloc, path, None, query, None))


def get_task_xcom_url(dag_id, task_id, execution_date, scheme='http', netloc='localhost:8080'):
    scheme = scheme
    netloc = netloc
    path = 'admin/airflow/xcom'
    query = urllib.parse.urlencode({
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date
    })
    return urllib.parse.urlunparse((scheme, netloc, path, None, query, None))
