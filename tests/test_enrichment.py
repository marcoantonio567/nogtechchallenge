from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import requests

from nogtech_etl.transform import enrichment


# Simula uma resposta HTTP da BrasilAPI sem precisar chamar a internet.
class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


# Simula uma sessao requests e registra as URLs chamadas pelos testes.
class FakeSession:
    def __init__(self, responses: dict[str, FakeResponse]):
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, timeout: int):
        self.calls.append(url)
        assert timeout == 10
        return self.responses[url]


# Testa o uso do cache de CEP: se o CEP ja existe no cache, nao deve haver
# chamada HTTP externa.
def test_fetch_cep_data_uses_cache_without_http_call() -> None:
    cache = {"01001000": {"cidade": "Sao Paulo", "uf": "SP", "bairro": "Se"}}
    session = FakeSession({})

    result = enrichment.fetch_cep_data("01001000", cache, session)

    assert result == cache["01001000"]
    assert session.calls == []


# Testa tratamento de erro esperado: CEP inexistente retorna 404, mas nao quebra
# o pipeline; os campos de localizacao ficam nulos.
def test_fetch_cep_data_handles_404_without_failing_pipeline() -> None:
    cache = {}
    session = FakeSession(
        {"https://brasilapi.com.br/api/cep/v2/99999999": FakeResponse(404, {})}
    )

    result = enrichment.fetch_cep_data("99999999", cache, session)

    assert result == {"cidade": None, "uf": None, "bairro": None}
    assert cache["99999999"] == result


# Testa falha externa real: erro 500 da API deve gerar excecao para o Airflow
# aplicar a politica de retry da task.
def test_fetch_cep_data_raises_for_external_api_failure() -> None:
    cache = {}
    session = FakeSession(
        {"https://brasilapi.com.br/api/cep/v2/01001000": FakeResponse(500, {})}
    )

    with pytest.raises(requests.HTTPError):
        enrichment.fetch_cep_data("01001000", cache, session)


# Testa o caminho feliz da transformacao completa com BrasilAPI mockada:
# junta transacao + engajamento, enriquece CEP, marca feriado, anonimiza CPF e
# garante que nome_aluno nao aparece no relatorio final.
def test_transform_and_enrich_success_with_mocked_brasilapi(tmp_path, monkeypatch) -> None:
    transacoes_path = tmp_path / "stage_transacoes.json"
    engajamento_path = tmp_path / "stage_engajamento.json"
    relatorio_path = tmp_path / "relatorio_final.csv"
    cep_cache_path = tmp_path / "ceps_cache.json"
    feriados_cache_path = tmp_path / "feriados_cache.json"

    pd.DataFrame(
        [
            {
                "id_transacao": "T001",
                "cpf_aluno": "123.456.789-09",
                "cpf_padronizado": "123.456.789-09",
                "data_transacao": "2024-01-01",
                "mes_referencia": "2024-01",
                "valor": 100.0,
                "curso": "Python Pro",
                "cep_cobranca": "01001-000",
            }
        ]
    ).to_json(transacoes_path, orient="records")
    pd.DataFrame(
        [
            {
                "cpf_padronizado": "123.456.789-09",
                "mes_referencia": "2024-01",
                "horas_assistidas": 8,
                "percentual_conclusao": 75,
            }
        ]
    ).to_json(engajamento_path, orient="records")

    session = FakeSession(
        {
            "https://brasilapi.com.br/api/cep/v2/01001000": FakeResponse(
                200,
                {
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighborhood": "Se",
                },
            ),
            "https://brasilapi.com.br/api/feriados/v1/2024": FakeResponse(
                200,
                [{"date": "2024-01-01"}],
            ),
        }
    )

    monkeypatch.setattr(enrichment, "RELATORIO_FINAL_PATH", relatorio_path)
    monkeypatch.setattr(enrichment, "CEP_CACHE_PATH", cep_cache_path)
    monkeypatch.setattr(enrichment, "FERIADOS_CACHE_PATH", feriados_cache_path)
    monkeypatch.setattr(enrichment, "build_http_session", lambda: session)
    monkeypatch.setattr(enrichment, "ensure_runtime_dirs", lambda: None)

    result = enrichment.transform_and_enrich(
        {
            "transacoes_path": str(transacoes_path),
            "engajamento_path": str(engajamento_path),
        }
    )

    report = pd.read_csv(Path(result["relatorio_final_path"]))

    assert result["registros_transformados"] == 1
    assert report.loc[0, "cpf_aluno"] == "***.456.789-**"
    assert report.loc[0, "cidade"] == "Sao Paulo"
    assert report.loc[0, "uf"] == "SP"
    assert report.loc[0, "venda_em_feriado"]
    assert "nome_aluno" not in report.columns
