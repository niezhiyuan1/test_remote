
from celery import Celery
from kombu import Queue
import multiprocessing
dagster_celery_name = "dagster_queue"
app = Celery('lite_execution',
             broker="amqp://admin:admin@10.122.83.86:5672//healthcheck",
             include=['dagster_celery_job.tasks.dagster_task'])
app.conf.update(
    CELERY_TASK_SERIALIZER='json',
    CELERY_ACCEPT_CONTENT=['json'],
    CELERY_RESULT_SERIALIZER='json',
    CELERY_TIMEZONE='Asia/Shanghai',
    CELERY_ENABLE_UTC=True,
    CELERYD_CONCURRENCY=multiprocessing.cpu_count(),  # celery worker的并发数 也是命令行-c指定的数目
    CELERYD_PREFETCH_MULTIPLIER=1,  # celery worker 每次去rabbitmq取任务的数量
    CELERYD_MAX_TASKS_PER_CHILD=40,  # 每个worker执行了多少任务就会死掉
    CELERY_QUEUES=(
        Queue(dagster_celery_name, routing_key=dagster_celery_name),
    ),
    CELERY_ROUTES={
        'dagster_celery_job.tasks.dagster_task.dagster_task': {
            'queue': dagster_celery_name,
            'routing_key': dagster_celery_name,
            'priority': 1
        }

    }

)
