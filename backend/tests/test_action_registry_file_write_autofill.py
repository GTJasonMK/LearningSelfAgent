import unittest
from unittest.mock import patch


class TestActionRegistryFileWriteAutofill(unittest.TestCase):
    def test_autofill_csv_content_from_latest_parse_input(self):
        from backend.src.actions.registry import _exec_file_write

        captured = {}

        def fake_execute_file_write(payload: dict, context=None):
            captured['payload'] = dict(payload or {})
            captured['context'] = dict(context or {})
            return {'ok': True}, None

        context = {
            'latest_parse_input_text': 'date,price_cny_per_gram\n2026-03-01,681.2\n2026-03-02,682.5\n'
        }
        payload = {'path': 'gold_prices.csv'}

        with patch('backend.src.actions.registry.execute_file_write', side_effect=fake_execute_file_write):
            result, error = _exec_file_write(1, 2, {'id': 3, 'title': 'file_write:gold_prices.csv'}, payload, context)

        self.assertIsNone(error)
        self.assertEqual({'ok': True}, result)
        self.assertIn('2026-03-02,682.5', str(captured['payload'].get('content') or ''))
        self.assertTrue(bool(context.get('file_write_content_auto_filled')))

    def test_skip_autofill_when_latest_parse_input_not_csv(self):
        from backend.src.actions.registry import _exec_file_write

        captured = {}

        def fake_execute_file_write(payload: dict, context=None):
            captured['payload'] = dict(payload or {})
            return {'ok': True}, None

        context = {
            'latest_parse_input_text': '这是网页正文，不是 CSV 内容'
        }
        payload = {'path': 'gold_prices.csv'}

        with patch('backend.src.actions.registry.execute_file_write', side_effect=fake_execute_file_write):
            result, error = _exec_file_write(1, 2, {'id': 3, 'title': 'file_write:gold_prices.csv'}, payload, context)

        self.assertIsNone(error)
        self.assertEqual({'ok': True}, result)
        self.assertNotIn('content', captured['payload'])
        self.assertFalse(bool(context.get('file_write_content_auto_filled')))


if __name__ == '__main__':
    unittest.main()
