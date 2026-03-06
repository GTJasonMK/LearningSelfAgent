import unittest
from unittest.mock import patch


class TestActionRegistryJsonParseAutofill(unittest.TestCase):
    def test_autofill_json_parse_text_from_latest_parse_input(self):
        from backend.src.actions.registry import _exec_json_parse

        captured = {}

        def fake_execute_json_parse(payload: dict, context=None):
            captured['payload'] = dict(payload or {})
            captured['context'] = dict(context or {})
            return {'ok': True}, None

        context = {
            'latest_parse_input_text': '{"rows":[{"date":"2026-03-01","price_cny_per_gram":681.2}]}'
        }
        payload = {'pick_keys': ['rows']}

        with patch('backend.src.actions.registry.execute_json_parse', side_effect=fake_execute_json_parse):
            result, error = _exec_json_parse(1, 2, {'id': 3, 'title': 'json_parse:解析API响应'}, payload, context)

        self.assertIsNone(error)
        self.assertEqual({'ok': True}, result)
        self.assertIn('price_cny_per_gram', str(captured['payload'].get('text') or ''))
        self.assertTrue(bool(context.get('json_parse_text_auto_filled')))

    def test_validate_action_object_allows_json_parse_missing_text_for_runtime_autofill(self):
        from backend.src.actions.registry import validate_action_object

        error = validate_action_object(
            {
                'action': {
                    'type': 'json_parse',
                    'payload': {'pick_keys': ['rows']},
                }
            }
        )

        self.assertIsNone(error)


if __name__ == '__main__':
    unittest.main()
