from datetime import timedelta

from airflow.models.dag import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from dag_converter.conversion.exceptions import InvalidSchedule


def convert_schedule(dag_object: DAG) -> str | None:
    """Return the converted schedule"""
    schedule = dag_object.schedule

    if schedule is None:
        return None
    if isinstance(schedule, str):
        return schedule
    elif isinstance(schedule, CronTriggerTimetable):
        return schedule._expression
    elif isinstance(schedule, timedelta):
        raise InvalidSchedule(f"Time delta based schedule is not supported: {schedule}")

    raise InvalidSchedule(f"Schedule is not valid: {schedule}")
