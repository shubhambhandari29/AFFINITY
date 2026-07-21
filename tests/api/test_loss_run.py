import asyncio

from fastapi.responses import FileResponse

from api.sac import loss_run


def test_download_loss_run_returns_generated_workbook(tmp_path, monkeypatch):
    output_path = tmp_path / "Customer_2026_07_16.xlsx"
    output_path.touch()

    async def fake_generate(customer_num):
        assert customer_num == "00123"
        return output_path

    monkeypatch.setattr(loss_run, "generate_loss_run", fake_generate)

    response = asyncio.run(loss_run.download_loss_run("00123"))

    assert isinstance(response, FileResponse)
    assert response.path == output_path
    assert response.media_type == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
