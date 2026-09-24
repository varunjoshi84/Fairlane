import pytest
from fairlane.metrics import (
    TASKS_SUBMITTED,
    TASKS_COMPLETED,
    inc_tasks_submitted,
    inc_tasks_completed
)

def test_metrics_registration_and_increment():
    # Record current value for a specific label set
    before_submit = TASKS_SUBMITTED.labels(tenant="test_tenant", priority="1")._value.get()
    inc_tasks_submitted("test_tenant", 1)
    after_submit = TASKS_SUBMITTED.labels(tenant="test_tenant", priority="1")._value.get()
    assert after_submit == before_submit + 1

    before_complete = TASKS_COMPLETED.labels(tenant="test_tenant", task_type="demo", status="succeeded")._value.get()
    inc_tasks_completed("test_tenant", "demo", "succeeded")
    after_complete = TASKS_COMPLETED.labels(tenant="test_tenant", task_type="demo", status="succeeded")._value.get()
    assert after_complete == before_complete + 1
