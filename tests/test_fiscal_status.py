import unittest
from datetime import datetime

from modules.export_utils import serialize_export_value
from modules.fiscal_status import (
    build_sql_queue_status_expr,
    compute_base_calculation_status,
    compute_final_note_status,
    compute_queue_state,
    normalize_manual_queue_status,
)


class FiscalStatusTests(unittest.TestCase):
    def test_base_calculo_nao_diverge_quando_base_e_menor_que_total(self):
        self.assertEqual(compute_base_calculation_status(80.0, 100.0), "ok")

    def test_status_final_considera_base_calculo_divergente(self):
        payload = {
            "status_base_calculo": "divergente",
            "status_simples_nacional": "ok",
            "status_csrf": "ok",
            "status_irrf": "ok",
            "status_inss": "ok",
            "status_valor_liquido": "ok",
        }
        self.assertEqual(compute_final_note_status(payload), "divergente")

    def test_status_final_mantem_divergencia_real(self):
        payload = {
            "status_simples_nacional": "ok",
            "status_csrf": "ok",
            "status_irrf": "divergente",
            "status_inss": "ok",
            "status_valor_liquido": "ok",
        }
        self.assertEqual(compute_final_note_status(payload), "divergente")

    def test_fila_manual_correta_remove_divergencia_operacional_final(self):
        payload = {
            "status_fila_manual": "correta",
            "status_simples_nacional": "divergente",
            "status_csrf": "ok",
            "status_irrf": "divergente",
            "status_inss": "ok",
            "status_base_calculo": "ok",
            "status_valor_liquido": "ok",
            "campos_ausentes_xml": "Valor Total",
            "alertas_fiscais": "Alerta antigo",
        }
        estado = compute_queue_state(payload)
        self.assertEqual(estado["status_fila_final"], "correta")
        self.assertFalse(estado["divergencia_fila_final"])
        self.assertEqual(estado["divergencia_fila_label"], "Sem divergência")

    def test_fila_sem_override_manual_considera_campos_e_simples(self):
        payload = {
            "status_fila_manual": None,
            "status_simples_nacional": "divergente",
            "status_csrf": "ok",
            "status_irrf": "ok",
            "status_inss": "ok",
            "status_base_calculo": "ok",
            "status_valor_liquido": "ok",
            "campos_ausentes_xml": "",
            "alertas_fiscais": "",
        }
        estado = compute_queue_state(payload)
        self.assertEqual(estado["status_fila_final"], "divergente")
        self.assertTrue(estado["divergencia_fila_final"])

    def test_alerta_fiscal_sozinho_nao_define_divergencia_final(self):
        payload = {
            "status_fila_manual": None,
            "status_simples_nacional": "ok",
            "status_csrf": "ok",
            "status_irrf": "ok",
            "status_inss": "ok",
            "status_base_calculo": "ok",
            "status_valor_liquido": "ok",
            "campos_ausentes_xml": "",
            "alertas_fiscais": "Alerta técnico",
        }
        estado = compute_queue_state(payload)
        self.assertEqual(estado["status_fila_final"], "correta")
        self.assertFalse(estado["divergencia_fila_final"])
        self.assertTrue(estado["possui_alertas_fiscais"])
        self.assertEqual(estado["divergencia_fila_label"], "Sem divergência")

    def test_normaliza_status_manual_para_valores_canonicos(self):
        self.assertEqual(normalize_manual_queue_status("ok"), "correta")
        self.assertEqual(normalize_manual_queue_status(" Divergente "), "divergente")
        self.assertIsNone(normalize_manual_queue_status("  "))

    def test_export_preserva_zero_e_datas(self):
        self.assertEqual(serialize_export_value(0), "0")
        self.assertEqual(serialize_export_value(0.0), "0.0")
        self.assertEqual(serialize_export_value(datetime(2026, 1, 2, 3, 4, 5)), "2026-01-02T03:04:05")
        self.assertEqual(serialize_export_value(None), "—")


    def test_notas_repo_status_expr_integration(self):
        from modules.notas_repo import STATUS_EXPR, STATUS_FILA_EXPR
        from modules.fiscal_status import build_sql_status_expr
        expected = build_sql_status_expr("n").strip()
        self.assertEqual(STATUS_EXPR.strip(), expected, "STATUS_EXPR deve usar regra centralizada")
        self.assertEqual(
            STATUS_FILA_EXPR.strip(),
            build_sql_queue_status_expr("n", status_expr=STATUS_EXPR).strip(),
            "STATUS_FILA_EXPR deve usar regra consolidada centralizada",
        )

if __name__ == "__main__":
    unittest.main()
