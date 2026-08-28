from __future__ import annotations

from pathlib import Path


def _workflow() -> str:
    return Path(".github/workflows/daily-snapshot.yml").read_text(encoding="utf-8")


def test_recovery_workflow_verifies_and_seals_before_ready_publication() -> None:
    workflow = _workflow()

    analyst_verify = workflow.index("- name: Verify snapshot")
    market_verify = workflow.index("- name: Verify prospective market context")
    seal = workflow.index("- name: Seal completed-session recovery bundle")
    publish = workflow.index("- name: Publish verified recovery bundle to Dropbox")
    final_gate = workflow.index("- name: Enforce recovery publication result")

    assert analyst_verify < seal
    assert market_verify < seal < publish < final_gate
    assert "upload-recovery-bundle" in workflow
    assert "run: python -m analyst_snapshot upload-dropbox" not in workflow


def test_manual_fresh_workflow_isolates_preclose_partitions() -> None:
    workflow = _workflow()

    assert 'default: "true"' in workflow
    assert 'snapshot_dir="$RUNNER_TEMP/analyst-recovery/archive"' in workflow
    assert 'echo "resume_arg="' in workflow
    assert "--session-date $OVERRIDE" in workflow
    assert "Requested run_date is not a completed XNYS session" in workflow
