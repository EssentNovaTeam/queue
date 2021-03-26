# Copyright (C) 2018 DynApps <http://www.dynapps.be>
# @author Stefan Rijnhart <stefan.rijnhart@dynapps.nl>
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).
from odoo.exceptions import UserError
from odoo.tests.common import SavepointCase


class TestLoadIndicator(SavepointCase):
    @classmethod
    def setUpClass(cls):
        super(TestLoadIndicator, cls).setUpClass()
        cls.env["database.load.indicator"].search([]).write({"active": False})
        cls.env.cr.execute(
            """
CREATE OR REPLACE FUNCTION load_indicator_test1(IN cutoff INTEGER, OUT INTEGER)
LANGUAGE SQL AS $$
    SELECT cutoff;
$$;
            CREATE OR REPLACE FUNCTION load_indicator_test2(OUT BOOLEAN)
LANGUAGE SQL AS $$
    SELECT FALSE;
$$;"""
        )

    def test_01_load_indicator(self):
        """Create two different indicators with different priorities. Check
        the passing of arguments and the try output, and the multiplication
        when the indicator is called."""
        self.assertFalse(self.env["database.load.indicator"]._get_active_indicator())
        indicator = self.env["database.load.indicator"].create(
            {
                "function": "load_indicator_test1",
                "arguments": "(5,)",
                "factor": 2,
                "name": "Transaction load test1",
                "sleep": 3,
                "sequence": 10,
            }
        )
        self.assertIn("SELECT cutoff", indicator.definition)
        with self.assertRaisesRegex(UserError, "Function returned 5"):
            indicator.try_call()
        active_indicator = self.env["database.load.indicator"]._get_active_indicator()
        self.assertEqual(active_indicator["name"], "Transaction load test1")
        self.assertEqual(
            self.env["database.load.indicator"]._call(active_indicator), 10
        )

        indicator2 = self.env["database.load.indicator"].create(
            {
                "function": "load_indicator_test2",
                "factor": -3,
                "name": "Transaction load test2",
                "sleep": 3,
                "sequence": 1,
            }
        )
        self.assertIn("SELECT FALSE", indicator2.definition)
        with self.assertRaisesRegex(UserError, "Function returned False"):
            indicator2.try_call()
        active_indicator = self.env["database.load.indicator"]._get_active_indicator()
        self.assertEqual(active_indicator["name"], "Transaction load test2")
        self.assertEqual(self.env["database.load.indicator"]._call(active_indicator), 3)

        indicator2.debug = True
        self.assertEqual(self.env["database.load.indicator"]._call(active_indicator), 3)
