from __future__ import annotations

from pathlib import Path


def _workflow() -> str:
    return Path(".github/workflows/daily-snapshot.yml").read_text(encoding="utf-8")


def test_recovery_workflow_verifies_and_seals_before_ready_publication() -> None:
    workflow = _workflow()

    price_capture = workflow.index("- name: Capture Daily price recovery tail")
    price_verify = workflow.index("- name: Verify Daily price recovery tail")
    price_seal = workflow.index("- name: Seal independent Daily price recovery")
    price_publish = workflow.index("- name: Publish independent Daily price recovery to Dropbox")
    analyst_run = workflow.index("- name: Run analyst snapshot")
    analyst_verify = workflow.index("- name: Verify snapshot")
    market_verify = workflow.index("- name: Verify prospective market context")
    seal = workflow.index("- name: Seal completed-session recovery bundle")
    publish = workflow.index("- name: Publish verified recovery bundle to Dropbox")
    price_gate = workflow.index("- name: Enforce independent price publication result")
    final_gate = workflow.index("- name: Enforce full recovery publication result")

    assert price_capture < price_verify < price_seal < price_publish < analyst_run
    assert price_verify < analyst_verify < seal
    assert market_verify < seal < publish < price_gate < final_gate
    assert "steps.verify_daily_prices.outcome == 'success'" in workflow
    assert "PRICE_VERIFY: ${{ steps.verify_daily_prices.outcome }}" in workflow
    assert "upload-recovery-bundle" in workflow
    assert "upload-price-recovery-bundle" in workflow
    assert "GENERATION_ID: price_gh_${{ github.run_id }}_${{ github.run_attempt }}" in workflow
    assert "GENERATION_ID: full_gh_${{ github.run_id }}_${{ github.run_attempt }}" in workflow
    assert "run: python -m analyst_snapshot upload-dropbox" not in workflow


def test_manual_fresh_workflow_isolates_preclose_partitions() -> None:
    workflow = _workflow()

    assert 'default: "true"' in workflow
    assert 'snapshot_dir="$RUNNER_TEMP/analyst-recovery/archive"' in workflow
    assert 'echo "resume_arg="' in workflow
    assert "--session-date $OVERRIDE" in workflow
    assert "Requested run_date is not a completed XNYS session" in workflow


def test_workflow_uses_dst_safe_1830_new_york_schedule_gate() -> None:
    workflow = _workflow()

    assert workflow.count('- cron: "30 22 * * 1-5"') == 1
    assert workflow.count('- cron: "30 23 * * 1-5"') == 1
    assert 'cron: "0 2 * * *"' not in workflow
    assert "EVENT_NAME: ${{ github.event_name }}" in workflow
    assert "EVENT_SCHEDULE: ${{ github.event.schedule || '' }}" in workflow
    assert "python -m analyst_snapshot schedule-gate" in workflow
    assert "GATED_DATE: ${{ steps.schedule_gate.outputs.new_york_date }}" in workflow
    assert 'session_arg="--session-date $GATED_DATE"' in workflow

    gate = workflow.index("- name: Authorize 18:30 America/New_York schedule")
    calendar = workflow.index("- name: Resolve latest completed XNYS session")
    assert gate < calendar
    assert "if: steps.schedule_gate.outputs.run == 'true'" in workflow[gate : calendar + 200]


def test_workflow_price_capture_is_serial_batched_and_not_used_for_symbol_smoke() -> None:
    workflow = _workflow()

    assert 'PRICE_BATCH_SIZE: "50"' in workflow
    assert "python -m analyst_snapshot daily-prices $RESUME_ARG" in workflow
    assert '--batch-size "$PRICE_BATCH_SIZE"' in workflow
    assert "python -m analyst_snapshot verify-daily-prices" in workflow
    price_step = workflow.index("- name: Capture Daily price recovery tail")
    price_verify = workflow.index("- name: Verify Daily price recovery tail")
    assert (
        "if: steps.calendar.outputs.run == 'true' && !inputs.symbols"
        in workflow[price_step:price_verify]
    )
    assert "timeout-minutes: 355" in workflow
    assert "timeout-minutes: 180" in workflow[price_step:price_verify]
    analyst_step = workflow.index("- name: Run analyst snapshot")
    commit_step = workflow.index("- name: Commit archive updates")
    assert "timeout-minutes: 120" in workflow[analyst_step:commit_step]

    ignored = Path(".gitignore").read_text(encoding="utf-8").splitlines()
    assert "/archive/daily_prices/" in ignored
    assert "/archive/_daily_price_manifests/" in ignored
    assert "/archive/_daily_price_checkpoints/" in ignored
    assert "/archive/_price_recovery_manifests/" in ignored


def test_price_only_is_typed_and_skips_every_full_bundle_step() -> None:
    workflow = _workflow()
    price_input = workflow.index("      price_only:")
    permissions = workflow.index("permissions:")
    input_block = workflow[price_input:permissions]
    assert "default: false" in input_block
    assert "type: boolean" in input_block
    assert "github.event.inputs.price_only" not in workflow
    assert "inputs.price_only == true && inputs.symbols != ''" in workflow

    full_step_names = (
        "Capture free prospective market context",
        "Run analyst snapshot",
        "Commit archive updates",
        "Verify snapshot",
        "Verify prospective market context",
        "Seal completed-session recovery bundle",
        "Enforce full recovery publication result",
    )
    for step_name in full_step_names:
        start = workflow.index(f"- name: {step_name}")
        next_step = workflow.find("\n      - name:", start + 1)
        block = workflow[start : next_step if next_step >= 0 else len(workflow)]
        assert "inputs.price_only != true" in block, step_name

    assert "(steps.publish_price_recovery.outcome == 'success' || inputs.symbols != '')" in workflow
    assert "price-recovery-bundle-*.json" in workflow
