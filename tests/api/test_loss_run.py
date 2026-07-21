import asyncio

from api.loss_run import loss_run


def test_generate_all_loss_runs_calls_batch_service(monkeypatch):
    async def fake_generate(customer_nums=None):
        assert customer_nums is None
        return {"generatedCount": 2}

    monkeypatch.setattr(loss_run, "generate_loss_runs", fake_generate)

    assert asyncio.run(loss_run.generate_all_loss_runs()) == {"generatedCount": 2}


def test_generate_selected_loss_runs_passes_customer_array(monkeypatch):
    captured = {}

    async def fake_generate(customer_nums=None):
        captured["customer_nums"] = customer_nums
        return {"generatedCount": 1}

    monkeypatch.setattr(loss_run, "generate_loss_runs", fake_generate)
    payload = loss_run.LossRunSelection(customerNumbers=["00123"])

    assert asyncio.run(loss_run.generate_selected_loss_runs(payload)) == {"generatedCount": 1}
    assert captured["customer_nums"] == ["00123"]
