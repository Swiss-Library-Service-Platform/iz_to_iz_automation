import speibiutils.speibiutils as speibi
import logging
from datetime import datetime
import speibiutils.transferprocess as tp


def task_workflow_new_to_ready() -> None:
    """Update the task summary

    Returns
    -------
    None
    """
    task_summary = speibi.TaskSummary()
    task_summary.clean_remote_directories()
    task_summary.clean_local_directories()
    task_summary.check_forms_conformity()
    task_summary.save()


def start(size: str) -> None:
    """Start the workflow

    Returns
    -------
    None
    """
    speibi.LogFile()
    task_workflow_new_to_ready()
    task_summary = speibi.TaskSummary()
    if task_summary.get_processing_task() is not None:
        logging.warning('Processing task already exists')
        return
    next_task = task_summary.get_next_task(size=size)
    if next_task is None:
        logging.warning('No task to process')
        return
    next_task = task_summary.update_task_state(next_task, new_state='PROCESSING')
    task_summary.save()
    logging.info(f'Next task: {next_task.get_name()} => process will start now')
    process_task(next_task)
    ended_task = task_summary.get_processing_task()
    logging.info(f'Task {ended_task.get_name()} => process ended')

    task_summary.update_task_state(ended_task,
                                   new_state='DONE',
                                   parameters={'End_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    speibi.LogFile.close_log()


def process_task(task: speibi.Task) -> None:
    """Process a task

    Parameters
    ----------
    task : speibi.Task
        Task to process

    Returns
    -------
    None
    """
    speibi.LogFile(task=task, file_name=task.get_name())
    logging.info(f'START processing task {task.get_name()}')
    tp.process_task(task)
    logging.info(f'END processing task {task.get_name()}')
    speibi.LogFile()
