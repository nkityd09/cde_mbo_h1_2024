from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

################## CML OPERATOR CODE START #######################
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook
import time

class CMLJobRunOperator(BaseOperator):

    def __init__(
            self,
            project: str,
            job: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.project = project
        self.job = job

    def execute(self, context):
        job_label = '({}/{})'.format(self.project, self.job)

        get_hook = HttpHook(http_conn_id='cml_rest_api', method='GET')
        post_hook = HttpHook(http_conn_id='cml_rest_api', method='POST')
        projects_url = 'api/v2/projects'
        r = get_hook.run(endpoint=projects_url)
        projects = {p['name'] : p['id'] for p in r.json()['projects']} if r.ok else None
        print(projects)
        
        if projects and self.project in projects.keys():
            jobs_url = '{}/{}/jobs'.format(projects_url, projects[self.project])
            print(jobs_url)
            r = get_hook.run(endpoint=jobs_url)
            jobs = {j['name'] : j['id'] for j in r.json()['jobs']} if r.ok else None
            
            if jobs and self.job in jobs.keys():
                runs_url = '{}/{}/runs'.format(jobs_url, jobs[self.job])
                r = post_hook.run(endpoint=runs_url)
                run = r.json() if r.ok else None
        
                if run:
                    status = run['status']
                    RUNNING_STATES = ['ENGINE_SCHEDULING', 'ENGINE_STARTING', 'ENGINE_RUNNING']
                    SUCCESS_STATES = ['ENGINE_SUCCEEDED']
                    POLL_INTERVAL = 10
                    while status and status in RUNNING_STATES:
                        run_id_url = '{}/{}'.format(runs_url, run['id'])
                        r = get_hook.run(endpoint=run_id_url)
                        status = r.json()['status'] if r.ok else None
                        time.sleep(POLL_INTERVAL)
                    if status not in SUCCESS_STATES:
                        raise AirflowException('Error while waiting for CML job ({}) to complete'
                            .format(job_label))
                else:
                    raise AirflowException('Problem triggering CML job ({})'.format(job_label))
            else:
                raise AirflowException('Problem finding the CML job ID ({})'.format(self.job))
        else:
            raise AirflowException('Problem finding the CML project ID ({})'.format(self.project))

class CMLApplicationRestartOperator(BaseOperator):
    def __init__(self, project, application_id, **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.application_id = application_id

    def execute(self, context):
        get_hook = HttpHook(http_conn_id='cml_rest_api', method='GET')
        post_hook = HttpHook(http_conn_id='cml_rest_api', method='POST')
        projects_url = 'api/v2/projects'
        r = get_hook.run(endpoint=projects_url)
        projects = {p['name'] : p['id'] for p in r.json()['projects']} if r.ok else None
        
        if projects and self.project in projects.keys():
            application_url = '{}/{}/applications'.format(projects_url, projects[self.project])
            r = get_hook.run(endpoint=application_url)
            applications = {j['name'] : j['id'] for j in r.json()['applications']} if r.ok else None
            print(applications)

            if applications and self.application_id in applications.keys():
                runs_url = '{}/{}:restart'.format(application_url, applications[self.application_id])
                r = post_hook.run(endpoint=runs_url)
                run = r.json() if r.ok else None
                print(run)

        # # Restart the application
        # r = post_hook.run(endpoint=restart_url)
        # if not r.ok:
        #     raise AirflowException(f"Failed to restart application: {r.status_code} - {r.text}")

        # restart_response = r.json()
        # restart_id = restart_response.get('id')

        # # Check the restart status periodically
        # status_url = f"api/v2/restarts/{restart_id}"
        # while True:
        #     r = get_hook.run(endpoint=status_url)
        #     if not r.ok:
        #         raise AirflowException(f"Failed to get restart status: {r.status_code} - {r.text}")

        #     restart_status = r.json().get('status')
        #     if restart_status in ['RESTARTING', 'RESTARTED']:
        #         time.sleep(5)
        #     elif restart_status == 'FAILED':
        #         raise AirflowException(f"Application restart failed: {restart_id}")
        #     elif restart_status == 'SUCCEEDED':
        #         self.log.info(f"Application {self.application_id} restarted successfully")
        #         break

################## CML OPERATOR CODE END #######################


default_args = {
    'owner': 'ankity',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
dag = DAG(
    'FinNews_LLM_Pipeline',
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
    #schedule_interval = '0 */3 * * *'
)
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

get_data = CDEJobRunOperator(
        task_id='Scrape_Data',
        dag=dag,
        job_name='finviz-job'
)

cml_clean_job = CMLJobRunOperator(
    task_id='Clean_Data',
    project='Original_MBO_Project',
    job='clean_data_job', 
    dag=dag)

cml_vecdb_job = CMLJobRunOperator(
    task_id='Populate_Vector_DB',
    project='Original_MBO_Project',
    job='Populate Vector DB with documents embeddings', 
    dag=dag)

cml_restart_app = CMLApplicationRestartOperator(
    task_id='Restart_Application',
    project='Original_MBO_Project',
    application_id='CML LLM Chatbot',
    dag=dag)

start >> get_data >> cml_clean_job >> cml_vecdb_job >> cml_restart_app >> end
