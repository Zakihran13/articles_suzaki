"""Schedules are kept separate from the jobs and stopped by default."""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from articles_suzaki.orchestration.jobs import daily_full_pipeline

daily_schedule = ScheduleDefinition(
    name="daily_full_pipeline_schedule",
    job=daily_full_pipeline,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC",
    run_config={"source": "api"},
    default_status=DefaultScheduleStatus.STOPPED,
    description="Optional UTC daily run; enable explicitly after configuring credentials.",
)
